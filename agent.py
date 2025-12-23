"""
Al-Mudeer - LangGraph InboxCRM Agent
Implements: Ingest -> Classify -> Extract -> Draft pipeline
Optimized for low bandwidth with text-only responses
"""

import json
import re
from typing import TypedDict, Literal, Optional, Dict, Any, List
from models import update_daily_analytics
from dataclasses import dataclass
import httpx
import os
import asyncio

# Helper to fetch URL content
async def fetch_url_content(url: str) -> Optional[str]:
    """Fetch content from a URL (max 2000 chars)"""
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            
            # Simple cleanup - remove HTML tags roughly
            text = resp.text
            # Remove scripts and styles
            text = re.sub(r'<script.*?>.*?</script>', '', text, flags=re.DOTALL)
            text = re.sub(r'<style.*?>.*?</style>', '', text, flags=re.DOTALL)
            # Remove tags
            text = re.sub(r'<[^>]+>', ' ', text)
            # Compress whitespace
            text = re.sub(r'\s+', ' ', text).strip()
            
            return text[:2000] # Limit context size
    except Exception as e:
        print(f"Failed to fetch URL {url}: {e}")
        return None

# LangGraph imports
from langgraph.graph import StateGraph, END

# Note: LLM configuration is centralized in services/llm_provider.py
# This file uses llm_generate() which handles OpenAI/Gemini failover

# Base system prompt for Arabic business context
BASE_SYSTEM_PROMPT = """أنت مساعد مكتبي ذكي للشركات في العالم العربي. تتحدث العربية الفصحى بأسلوب مهني ومهذب.
تفهم السياق المحلي جيداً (العملة، العادات، أسلوب التخاطب).
مهمتك هي تحليل الرسائل الواردة واستخراج المعلومات المهمة وصياغة ردود مناسبة.
كن موجزاً ومباشراً في ردودك لتوفير استهلاك البيانات."""


def build_system_prompt(preferences: Optional[Dict[str, Any]] = None) -> str:
    """
    Build a system prompt customized by workspace preferences.

    preferences comes from user_preferences table and may include:
    - tone: formal | friendly | custom
    - custom_tone_guidelines
    - business_name, industry, products_services
    - preferred_languages, reply_length, formality_level
    """
    if not preferences:
        return BASE_SYSTEM_PROMPT

    tone = (preferences.get("tone") or "formal").lower()
    custom_guidelines = (preferences.get("custom_tone_guidelines") or "").strip()

    # Tone description
    if tone == "friendly":
        tone_desc = "استخدم نبرة ودية وقريبة لكن مع احترام مهني، وتجنّب العامية الثقيلة."
    elif tone == "custom" and custom_guidelines:
        tone_desc = custom_guidelines
    else:
        # formal or unknown
        tone_desc = "استخدم نبرة رسمية بسيطة وواضحة بدون مبالغة في المجاملات."

    business_name = preferences.get("business_name") or "الشركة"
    industry = preferences.get("industry") or ""
    products = preferences.get("products_services") or ""

    business_context_parts = [f"تتحدث باسم {business_name}."]
    if industry:
        business_context_parts.append(f"النشاط الرئيسي: {industry}.")
    if products:
        business_context_parts.append(f"الخدمات / المنتجات الأساسية: {products}.")

    reply_length = (preferences.get("reply_length") or "").lower()
    if reply_length == "short":
        length_hint = "احرص أن يكون الرد قصيراً قدر الإمكان (من 2 إلى 3 أسطر تقريباً)."
    elif reply_length == "long":
        length_hint = "يمكن أن يكون الرد مفصلاً أكثر عند الحاجة، مع المحافظة على الوضوح."
    else:
        length_hint = "حافظ على طول رد متوسط وواضح (حوالي 3 إلى 6 أسطر)."

    return (
        BASE_SYSTEM_PROMPT
        + "\n\n"
        + "سياق العمل:\n"
        + " ".join(business_context_parts)
        + "\n\nأسلوب الكتابة المطلوب:\n"
        + tone_desc
        + "\n"
        + length_hint
    )


class AgentState(TypedDict):
    """State for the InboxCRM agent"""
    # Input
    raw_message: str
    message_type: str  # email, whatsapp, general
    
    # Classification
    intent: str  # استفسار, طلب خدمة, شكوى, متابعة, عرض, أخرى
    urgency: str  # عاجل, عادي, منخفض
    sentiment: str  # إيجابي, محايد, سلبي
    language: Optional[str]
    dialect: Optional[str]
    
    # Extraction
    sender_name: Optional[str]
    sender_contact: Optional[str]
    key_points: list[str]
    action_items: list[str]
    extracted_entities: dict  # dates, amounts, product names, etc.
    
    # Output
    summary: str
    draft_response: str
    suggested_actions: list[str]
    
    # Metadata
    error: Optional[str]
    processing_step: str

    # Preferences / context
    preferences: Optional[Dict[str, Any]]
    # Recent conversation history (plain text)
    conversation_history: Optional[str]
    
    # Multimodal support
    attachments: Optional[List[Dict[str, Any]]]


async def call_llm(
    prompt: str,
    system: Optional[str] = None,
    json_mode: bool = False,
    max_tokens: int = 600,
    attachments: Optional[List[Dict[str, Any]]] = None,
) -> Optional[str]:
    """
    Call LLM using multi-provider service with automatic failover.

    Provider chain: OpenAI -> Google Gemini -> Rule-based fallback
    
    Features:
    - Automatic failover between providers
    - Response caching to reduce API calls
    - Circuit breaker for failing providers
    - Exponential backoff for rate limiting
    
    Returns None if all providers fail (caller should use rule-based logic).
    """
    try:
        from services.llm_provider import llm_generate
        
        effective_system = system or BASE_SYSTEM_PROMPT
        
        response = await llm_generate(
            prompt=prompt,
            system=effective_system,
            json_mode=json_mode,
            max_tokens=max_tokens,
            temperature=0.3,
            attachments=attachments
        )
        
        return response
    except Exception as e:
        # If LLM fails, return None to trigger rule-based fallback
        print(f"LLM service error: {e}")
        return None



def rule_based_classify(message: str) -> dict:
    """Rule-based classification fallback (works offline)"""
    message_lower = message.lower()
    
    # Intent detection - order matters, more specific first
    intent = "أخرى"
    
    # Help/assistance requests (common pattern)
    if any(word in message for word in ["مساعد", "ساعد", "تساعد", "help", "أحتاج"]):
        intent = "طلب مساعدة"
    elif any(word in message for word in ["سعر", "كم", "تكلفة", "أسعار", "ثمن"]):
        intent = "استفسار"
    elif any(word in message for word in ["أريد", "أرغب", "طلب", "احتاج", "نريد", "أطلب"]):
        intent = "طلب خدمة"
    elif any(word in message for word in ["شكوى", "مشكلة", "لم يعمل", "تأخر", "سيء", "خطأ"]):
        intent = "شكوى"
    elif any(word in message for word in ["متابعة", "بخصوص", "استكمال", "تذكير"]):
        intent = "متابعة"
    elif any(word in message for word in ["عرض", "خصم", "تخفيض", "فرصة"]):
        intent = "عرض"
    # Marketing/Spam/Automated detection
    elif any(word in message for word in ["كود", "رمز تحقق", "otp", "code", "verification"]):
        intent = "آلي"
    elif any(word in message for word in ["اشترك", "اربح", "مجانا", "سحب", "جوائز", "تصفية"]):
        intent = "تسويق"
    # Detect greetings/casual messages
    elif any(word in message for word in ["مرحب", "السلام", "أهلا", "صباح", "مساء", "hi", "hello"]):
        intent = "تحية"
    
    # Urgency detection
    urgency = "عادي"
    if any(word in message for word in ["عاجل", "فوري", "اليوم", "الآن", "ضروري"]):
        urgency = "عاجل"
    elif any(word in message for word in ["لاحقاً", "عندما", "متى ما"]):
        urgency = "منخفض"
    
    # Sentiment detection
    sentiment = "محايد"
    if any(word in message for word in ["شكراً", "ممتاز", "رائع", "سعيد", "مسرور"]):
        sentiment = "إيجابي"
    elif any(word in message for word in ["غاضب", "محبط", "سيء", "مستاء", "للأسف"]):
        sentiment = "سلبي"
    
    return {"intent": intent, "urgency": urgency, "sentiment": sentiment}


def extract_entities(message: str) -> dict:
    """Extract entities using regex patterns"""
    entities = {}
    
    # Phone numbers (Syrian/Arabic format)
    phone_patterns = [
        r'(?:00963|\+963|0)?9\d{8}',  # Syrian mobile
        r'(?:00963|\+963|0)?11\d{7}',  # Damascus landline
        r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',  # General format
    ]
    phones = []
    for pattern in phone_patterns:
        phones.extend(re.findall(pattern, message))
    if phones:
        entities["phones"] = list(set(phones))
    
    # Email
    emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', message)
    if emails:
        entities["emails"] = emails
    
    # Dates (Arabic format)
    dates = re.findall(r'\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}', message)
    if dates:
        entities["dates"] = dates
    
    # Money amounts
    amounts = re.findall(r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:ل\.س|ليرة|دولار|\$|USD)', message)
    if amounts:
        entities["amounts"] = amounts
    
    # Extract possible name (after السيد/السيدة/الأستاذ)
    name_match = re.search(r'(?:السيد|السيدة|الأستاذ|الأستاذة|أخي|أختي)\s+([\u0600-\u06FF\s]+)', message)
    if name_match:
        entities["mentioned_name"] = name_match.group(1).strip()
    
    return entities


def generate_rule_based_response(state: dict) -> str:
    """Generate a human-like draft response based on intent"""
    intent = state.get("intent", "أخرى")
    sender = state.get("sender_name") or ""  # Don't use formal address for unknown
    raw_message = state.get("raw_message", "")
    
    # For short/simple messages, use conversational style
    if len(raw_message.strip()) < 50:
        # Short message - be conversational
        templates = {
            "طلب مساعدة": "مرحباً! 👋\nبالطبع، أنا هنا لمساعدتك.\nكيف يمكنني خدمتك اليوم؟",
            
            "تحية": "أهلاً وسهلاً! 😊\nسعيدون بتواصلك معنا.\nكيف نقدر نساعدك؟",
            
            "استفسار": "مرحباً!\nشكراً لاستفسارك.\nممكن توضح أكثر عن اللي تحتاجه؟",
            
            "طلب خدمة": "أهلاً!\nتمام، وصلنا طلبك.\nممكن تعطينا تفاصيل أكثر؟",
            
            "شكوى": "مرحباً،\nآسفين تسمع هذا الكلام! 😔\nممكن توضح لنا المشكلة بالتفصيل عشان نحلها بأسرع وقت؟",
            
            "أخرى": "مرحباً! 👋\nشكراً لتواصلك معنا.\nكيف نقدر نخدمك اليوم؟"
        }
    else:
        # Longer/formal messages - professional but still warm
        sender_greeting = f"أهلاً{' ' + sender if sender and sender != 'None' else ''}،" if sender and sender not in ['None', ''] else "مرحباً،"
        
        templates = {
            "طلب مساعدة": f"""{sender_greeting}

بالتأكيد نقدر نساعدك! ✨
وضّحنا برسالتك وسنقوم بالرد عليك بأفضل طريقة ممكنة.

نحن بخدمتك دائماً.""",
            
            "تحية": f"""{sender_greeting}

أهلاً وسهلاً بك! 😊
سعداء جداً بتواصلك معنا.

كيف يمكننا مساعدتك اليوم؟""",

            "استفسار": f"""{sender_greeting}

شكراً لتواصلك واستفسارك.

بخصوص ما ذكرته، سنقوم بتوفير المعلومات المطلوبة في أقرب وقت.
إذا كان لديك تفاصيل إضافية، شاركنا بها.

مع تحياتنا 🌟""",
            
            "طلب خدمة": f"""{sender_greeting}

شكراً لثقتك بنا! 💫

وصلنا طلبك وسنتواصل معك قريباً لاستكمال التفاصيل.

نسعد بخدمتك.""",
            
            "شكوى": f"""{sender_greeting}

نعتذر جداً عن أي إزعاج واجهته. 😔

ملاحظاتك مهمة جداً لنا وسنعمل على معالجة الموضوع بأولوية قصوى.
سيتواصل معك أحد فريقنا قريباً.

شكراً لصبرك.""",
            
            "متابعة": f"""{sender_greeting}

شكراً لمتابعتك معنا.

سنقوم بمراجعة الموضوع وإطلاعك على آخر المستجدات في أقرب فرصة.

نقدر تواصلك المستمر. 🙏""",
            
            "عرض": f"""{sender_greeting}

شكراً لتواصلك وعرضك الكريم.

سنقوم بدراسته والرد عليك قريباً.

مع التقدير.""",
            
            "أخرى": f"""{sender_greeting}

شكراً لتواصلك معنا! 🌟

وصلتنا رسالتك وسنقوم بمراجعتها والرد عليك في أقرب وقت.

نحن سعداء بخدمتك."""
        }
    
    return templates.get(intent, templates["أخرى"])


# ============ LangGraph Nodes ============

async def ingest_node(state: AgentState) -> AgentState:
    """Step 1: Ingest and clean the message"""
    state["processing_step"] = "استلام"
    
    # Update analytics for received message
    if state.get("preferences") and state["preferences"].get("license_key_id"):
        try:
            from models import update_daily_analytics
            # Note: We use asyncio.create_task to not block the agent flow
            import asyncio
            asyncio.create_task(update_daily_analytics(
                license_id=state["preferences"]["license_key_id"],
                messages_received=1
            ))
        except Exception as e:
            print(f"Analytics update failed: {e}")
    
    # Clean the message
    raw = state["raw_message"].strip()
    
    # Detect message type if not specified
    if not state.get("message_type"):
        if "@" in raw and "subject" in raw.lower():
            state["message_type"] = "email"
        elif any(x in raw for x in ["واتساب", "whatsapp", "📱"]):
            state["message_type"] = "whatsapp"
        else:
            state["message_type"] = "general"
            
    # Link Browsing: Detect and fetch URLs
    # Pattern for http/https URLs
    url_pattern = r'https?://[^\s<>"]+|www\.[^\s<>"]+'
    urls = re.findall(url_pattern, raw)
    
    if urls:
        # Fetch first URL only to save tokens/time
        url = urls[0]
        print(f"Detected URL: {url} - fetching content...")
        
        # We need to run this async, but ingest_node is async so it fits
        content = await fetch_url_content(url)
        
        if content:
            # Append to raw message as context for subsequent nodes
            state["raw_message"] += f"\n\n[System: Content fetching from {url}]\n{content}"
            print(f"Added {len(content)} chars of context from URL")
    
    return state


async def classify_node(state: AgentState) -> AgentState:
    """Step 2: Classify intent, urgency, and sentiment"""
    state["processing_step"] = "تصنيف"
    
    # Try LLM first – structured JSON output in Arabic business context
    history_block = ""
    if state.get("conversation_history"):
        history_block = f"\nسياق المحادثة السابقة مع هذا العميل (من الأحدث إلى الأقدم):\n{state['conversation_history']}\n"

    prompt = f"""أنت خبير خدمة عملاء يدعم العربية ولغات أخرى.
حلل الرسالة التالية وأعطني:
3. حلل الرسالة التالية وأعطني:
4. 1. النية (intent): استفسار، طلب خدمة، شكوى، متابعة، عرض، تسويق (للمتطفلين)، آلي (OTP/تنبيهات)، أو أخرى
2. الأهمية (urgency): عاجل، عادي، منخفض
3. المشاعر (sentiment): إيجابي، محايد، سلبي
4. اللغة (language): ar, en, fr, أو رمز ISO إن أمكن
5. اللهجة (dialect): سوري، سعودي، مصري، خليجي، فصحى، أو Other

استخدم السياق الطبيعي للمحادثة، وتجنب الحكم من كلمة واحدة فقط.
{history_block}
النص الحالي:
{state['raw_message']}

أرجع النتيجة بصيغة JSON فقط بهذا الشكل:
{{"intent": "استفسار", "urgency": "عادي", "sentiment": "محايد", "language": "ar", "dialect": "شامي"}}"""

    llm_response = await call_llm(
        prompt,
        system=build_system_prompt(state.get("preferences")),
        json_mode=True,
        attachments=state.get("attachments")
    )
    
    if llm_response:
        try:
            classification = json.loads(llm_response)
            state["intent"] = classification.get("intent", "أخرى")
            state["urgency"] = classification.get("urgency", "عادي")
            state["sentiment"] = classification.get("sentiment", "محايد")
            state["language"] = classification.get("language") or "ar"
            state["dialect"] = classification.get("dialect")
            return state
        except json.JSONDecodeError:
            pass
    
    # Fallback: Mark as pending retry (no rule-based fallback)
    # This ensures only Gemini-quality responses are used
    state["intent"] = "pending"
    state["urgency"] = "عادي"
    state["sentiment"] = "محايد"
    state["error"] = "LLM unavailable - will retry"
    
    return state


async def extract_node(state: AgentState) -> AgentState:
    """Step 3: Extract key information"""
    state["processing_step"] = "استخراج"
    
    # Extract entities using regex (reliable, no LLM needed)
    entities = extract_entities(state["raw_message"])
    state["extracted_entities"] = entities
    
    # Set sender info from entities if found
    if entities.get("mentioned_name"):
        state["sender_name"] = entities["mentioned_name"]
    if entities.get("emails"):
        state["sender_contact"] = entities["emails"][0]
    elif entities.get("phones"):
        state["sender_contact"] = entities["phones"][0]
    
    # Try LLM for key points extraction
    history_block = ""
    if state.get("conversation_history"):
        history_block = f"\nسياق المحادثة السابقة مع هذا العميل (من الأحدث إلى الأقدم):\n{state['conversation_history']}\n"

    prompt = f"""أنت مساعد يدعم فريق خدمة العملاء.
من الرسالة التالية استخرج باختصار:
1. النقاط الرئيسية التي يذكرها العميل (3 نقاط كحد أقصى).
2. أهم الإجراءات أو الخطوات التي ينبغي على الفريق القيام بها.

يجب أن تكون اللغة عربية فصحى بسيطة ومباشرة.
{history_block}
نص الرسالة الحالية:
{state['raw_message']}

أرجع النتيجة بصيغة JSON فقط بهذا الشكل:
{{"key_points": ["نقطة مختصرة 1", "نقطة مختصرة 2"], "action_items": ["إجراء واضح 1"]}}"""

    llm_response = await call_llm(
        prompt,
        system=build_system_prompt(state.get("preferences")),
        json_mode=True,
    )
    
    if llm_response:
        try:
            extracted = json.loads(llm_response)
            state["key_points"] = extracted.get("key_points", [])
            state["action_items"] = extracted.get("action_items", [])
            return state
        except json.JSONDecodeError:
            pass
    
    # Fallback: Skip extraction if LLM failed (will retry later)
    if not state.get("key_points"):
        state["key_points"] = []
    if not state.get("action_items"):
        state["action_items"] = []
    
    return state


async def draft_node(state: AgentState) -> AgentState:
    """Step 4: Draft a human-like response"""
    state["processing_step"] = "صياغة"
    
    # Fix None sender name
    sender = state.get("sender_name")
    if not sender or sender == "None":
        sender = ""  # Don't use formal address for unknown
    
    intent = state.get("intent", "أخرى")
    key_points = state.get("key_points", [])
    raw_message = state.get("raw_message", "")
    dialect = state.get("dialect", "فصحى")
    
    # Determine response style based on message length
    is_casual = len(raw_message.strip()) < 50
    
    # Build conversation history context
    history_block = ""
    if state.get("conversation_history"):
        history_block = f"\nPrevious conversation context:\n{state['conversation_history']}\n"
    
    # Get detected language
    language = state.get("language", "ar")
    
    # Build language and dialect instructions
    if language and language != "ar":
        # Non-Arabic language - respond in same language
        language_names = {
            "en": "English",
            "fr": "French", 
            "es": "Spanish",
            "de": "German",
            "tr": "Turkish",
        }
        lang_name = language_names.get(language, language.upper())
        
        prompt = f"""You are a friendly, professional customer service representative. You speak naturally like a real person, not a robot.

🎯 Your task: Write a natural, direct response to the customer's message.

🗣️ IMPORTANT: Respond in {lang_name} (the same language as the customer)!

✅ Do:
- Be friendly, direct, and natural
- Answer what the customer asked/requested directly
- Use simple, clear language
- You can use one or two emojis if appropriate 😊
{"- Be very concise (2-3 lines only)" if is_casual else "- Keep the response appropriate to the message length (4-6 lines)"}

❌ Don't:
- Don't use overly formal phrases like "Dear Sir/Madam"
- Don't say "Your message has been received" (boring and robotic)
- Don't end with "Customer Service Team" (too formal)
- Don't repeat the same routine phrases
- Don't say "I am an AI" or "I cannot" - just respond naturally

📝 Customer's message:
\"{raw_message}\"

📊 Message analysis:
- Type: {intent}
- Language: {lang_name}
- Key points: {', '.join(key_points) if key_points else 'General message'}
{f"- Customer name: {sender}" if sender else ""}
{history_block}

✍️ Write your response directly in {lang_name} (no explanation):"""

    else:
        # Arabic - handle dialects
        dialect_instruction = ""
        if dialect and dialect != "فصحى":
            dialect_examples = {
                "سعودي": "استخدم اللهجة السعودية/الخليجية في الرد. مثال: 'وش تحتاج؟'، 'تمام'، 'إن شاء الله'، 'يعطيك العافية'، 'كيف أقدر أساعدك؟'",
                "خليجي": "استخدم اللهجة الخليجية في الرد. مثال: 'شلونك؟'، 'زين'، 'واجد'، 'يا هلا'، 'كيف أقدر أخدمك؟'",
                "مصري": "استخدم اللهجة المصرية في الرد. مثال: 'إزيك؟'، 'تمام'، 'عايز إيه؟'، 'أقدر أساعدك إزاي؟'، 'الحقيقة'",
                "شامي": "استخدم اللهجة الشامية في الرد. مثال: 'كيفك؟'، 'شو بدك؟'، 'منيح'، 'هلق'، 'كتير منيح'",
                "سوري": "استخدم اللهجة السورية في الرد. مثال: 'شو بدك؟'، 'كيفك؟'، 'منيح'، 'هلق'، 'ليك'",
            }
            dialect_instruction = dialect_examples.get(dialect, f"استخدم لهجة {dialect} في الرد إن أمكن.")

        prompt = f"""أنت ممثل خدمة عملاء ودود وطبيعي. تتحدث مع العملاء كإنسان حقيقي، لست روبوتاً.

🎯 مهمتك: اكتب رداً طبيعياً ومباشراً على رسالة العميل.

🗣️ اللهجة المطلوبة: {dialect}
{dialect_instruction if dialect_instruction else "استخدم عربية فصحى مبسّطة وسهلة الفهم."}

✅ افعل:
- كن ودوداً ومباشراً وطبيعياً
- رد على ما سأله/طلبه العميل مباشرة
- طابق لهجة العميل في ردك (مهم جداً!)
- يمكنك استخدام إيموجي واحد أو اثنين إن مناسب 😊
- إذا سأل سؤالاً شخصياً، رد بلطف وحوّل الحديث للخدمات
{"- كن موجزاً جداً (سطرين أو ثلاثة فقط)" if is_casual else "- اجعل الرد ملائماً لطول الرسالة (4-6 أسطر)"}

❌ لا تفعل:
- لا تستخدم "السيد/السيدة المحترم/ة" (رسمي جداً!)
- لا تقل "تم استلام رسالتك" (ممل وروبوتي)
- لا تنه الرد بـ "فريق خدمة العملاء" (رسمي جداً)
- لا تكرر نفس الجمل الروتينية
- لا تقل "أنا ذكاء اصطناعي" أو "ليس لدي القدرة" - فقط رد بشكل طبيعي
- لا ترد بالفصحى الرسمية إذا كان العميل يستخدم لهجة محلية

📝 رسالة العميل:
\"{raw_message}\"

📊 تحليل الرسالة:
- نوع: {intent}
- اللهجة المكتشفة: {dialect}
- النقاط المهمة: {', '.join(key_points) if key_points else 'رسالة عامة'}
{f"- اسم العميل: {sender}" if sender else ""}
{history_block}

✍️ اكتب الرد مباشرة بنفس لهجة العميل (بدون أي شرح):"""

    llm_response = await call_llm(
        prompt,
        system=build_system_prompt(state.get("preferences")),
        json_mode=False,
        max_tokens=1200,  # Arabic needs more tokens - increased from 800
        attachments=state.get("attachments")
    )
    
    # Lower threshold to 15 - accept short but valid responses
    if llm_response and len(llm_response.strip()) > 15:
        state["draft_response"] = llm_response.strip()
    else:
        # No fallback to generic templates - use placeholder for retry
        # This ensures only Gemini-quality responses are shown to users
        state["draft_response"] = "⏳ جاري تحليل الرسالة تلقائياً..."
        state["error"] = "LLM unavailable - pending retry"
        
    # Update analytics for reply generation
    if state.get("preferences") and state["preferences"].get("license_key_id"):
        try:
            from models import update_daily_analytics
            # Note: We use asyncio.create_task to not block the agent flow
            import asyncio
            asyncio.create_task(update_daily_analytics(
                license_id=state["preferences"]["license_key_id"],
                messages_replied=1,
                sentiment=state.get("sentiment", "محايد")
            ))
        except Exception as e:
            print(f"Analytics reply update failed: {e}")
    
    # Generate a cleaner summary (avoid showing "None")
    sender_display = sender if sender else "عميل"
    state["summary"] = f"رسالة {intent} من {sender_display}. المشاعر: {state.get('sentiment', 'محايد')}. الأهمية: {state.get('urgency', 'عادي')}."
    
    # Suggested actions based on intent
    actions_map = {
        "طلب مساعدة": ["الرد على العميل", "توضيح الخدمات المتاحة"],
        "تحية": ["الترحيب بالعميل", "بدء المحادثة"],
        "استفسار": ["الرد على الاستفسار", "إضافة للأسئلة الشائعة"],
        "طلب خدمة": ["إنشاء طلب جديد", "تحديد موعد", "إرسال عرض سعر"],
        "شكوى": ["تصعيد للمدير", "فتح تذكرة دعم", "الاتصال بالعميل"],
        "متابعة": ["تحديث حالة الطلب", "إرسال تقرير"],
        "عرض": ["دراسة العرض", "تحويل للمشتريات"],
        "أخرى": ["مراجعة يدوية", "تصنيف الرسالة"]
    }
    state["suggested_actions"] = actions_map.get(intent, actions_map["أخرى"])
    
    return state


# ============ Build the Graph ============

def create_inbox_agent():
    """Create the InboxCRM LangGraph agent"""
    
    # Create the graph
    workflow = StateGraph(AgentState)
    
    # Add nodes
    workflow.add_node("ingest", ingest_node)
    workflow.add_node("classify", classify_node)
    workflow.add_node("extract", extract_node)
    workflow.add_node("draft", draft_node)
    
    # Define edges (linear pipeline)
    # Define conditional routing
    def route_after_classify(state: AgentState):
        """Route to extract or END based on intent"""
        intent = state.get("intent", "أخرى")
        if intent in ["تسويق", "آلي", "spam", "marketing", "automated"]:
            return "end"
        return "extract"

    # Define edges
    workflow.set_entry_point("ingest")
    workflow.add_edge("ingest", "classify")
    workflow.add_conditional_edges(
        "classify",
        route_after_classify,
        {
            "extract": "extract",
            "end": END
        }
    )
    workflow.add_edge("extract", "draft")
    workflow.add_edge("draft", END)
    
    # Compile
    return workflow.compile()


# Singleton agent instance
_agent = None

def get_agent():
    """Get or create the agent instance"""
    global _agent
    if _agent is None:
        _agent = create_inbox_agent()
    return _agent


async def process_message(
    message: str,
    message_type: str = None,
    sender_name: str = None,
    sender_contact: str = None,
    sender_city: str = None,
    preferences: Optional[Dict[str, Any]] = None,
    conversation_history: Optional[str] = None,
    attachments: Optional[List[Dict[str, Any]]] = None,
) -> dict:
    """Process a message through the InboxCRM pipeline"""
    
    agent = get_agent()
    
    # Initial state
    initial_state: AgentState = {
        "raw_message": message,
        "message_type": message_type or "general",
        "intent": "",
        "urgency": "",
        "sentiment": "",
        "sender_name": sender_name,
        "sender_contact": sender_contact,
        "key_points": [],
        "action_items": [],
        "extracted_entities": {},
        "summary": "",
        "draft_response": "",
        "suggested_actions": [],
        "error": None,
        "processing_step": "",
        "preferences": preferences,
        "preferences": preferences,
        "conversation_history": conversation_history,
        "attachments": attachments,
    }
    
    try:
        # Run the agent
        final_state = await agent.ainvoke(initial_state)
        return {
            "success": True,
            "data": {
                "intent": final_state["intent"],
                "urgency": final_state["urgency"],
                "sentiment": final_state["sentiment"],
                "language": final_state.get("language"),
                "dialect": final_state.get("dialect"),
                "sender_name": final_state["sender_name"],
                "sender_contact": final_state["sender_contact"],
                "key_points": final_state["key_points"],
                "action_items": final_state["action_items"],
                "extracted_entities": final_state["extracted_entities"],
                "summary": final_state["summary"],
                "draft_response": final_state["draft_response"],
                "suggested_actions": final_state["suggested_actions"],
                "message_type": final_state["message_type"]
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"حدث خطأ في المعالجة: {str(e)}"
        }

