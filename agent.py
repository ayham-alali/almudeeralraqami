"""
Al-Mudeer - LangGraph InboxCRM Agent
Implements: Ingest -> Classify -> Extract -> Draft pipeline
Optimized for low bandwidth with text-only responses
"""

import json
import re
from typing import TypedDict, Literal, Optional, Dict, Any
from dataclasses import dataclass
import httpx
import os

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


async def call_llm(
    prompt: str,
    system: Optional[str] = None,
    json_mode: bool = False,
    max_tokens: int = 600,
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
            temperature=0.3
        )
        
        return response
    except Exception as e:
        # If LLM fails, return None to trigger rule-based fallback
        print(f"LLM service error: {e}")
        return None



def rule_based_classify(message: str) -> dict:
    """Rule-based classification fallback (works offline)"""
    message_lower = message.lower()
    
    # Intent detection
    intent = "أخرى"
    if any(word in message for word in ["سعر", "كم", "تكلفة", "أسعار"]):
        intent = "استفسار"
    elif any(word in message for word in ["أريد", "أرغب", "طلب", "احتاج", "نريد"]):
        intent = "طلب خدمة"
    elif any(word in message for word in ["شكوى", "مشكلة", "لم يعمل", "تأخر", "سيء"]):
        intent = "شكوى"
    elif any(word in message for word in ["متابعة", "بخصوص", "استكمال", "تذكير"]):
        intent = "متابعة"
    elif any(word in message for word in ["عرض", "خصم", "تخفيض", "فرصة"]):
        intent = "عرض"
    
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
    """Generate a draft response based on intent"""
    intent = state.get("intent", "أخرى")
    sender = state.get("sender_name", "العميل الكريم")
    
    templates = {
        "استفسار": f"""السيد/السيدة {sender} المحترم/ة،

شكراً لتواصلكم معنا.

بخصوص استفساركم، نود إفادتكم بأن [أضف التفاصيل هنا].

نرحب بأي استفسارات إضافية.

مع أطيب التحيات،
فريق خدمة العملاء""",
        
        "طلب خدمة": f"""السيد/السيدة {sender} المحترم/ة،

شكراً لثقتكم بخدماتنا.

تم استلام طلبكم بنجاح وسيتم التواصل معكم قريباً لاستكمال الإجراءات.

للمتابعة أو الاستفسار، نحن بخدمتكم.

مع أطيب التحيات،
فريق المبيعات""",
        
        "شكوى": f"""السيد/السيدة {sender} المحترم/ة،

نعتذر عن أي إزعاج سببناه لكم.

تم تسجيل ملاحظاتكم وسيتم معالجة الموضوع بأقصى سرعة.
سنتواصل معكم خلال [حدد المدة] لإطلاعكم على المستجدات.

نقدر صبركم وتفهمكم.

مع أطيب التحيات،
فريق خدمة العملاء""",
        
        "متابعة": f"""السيد/السيدة {sender} المحترم/ة،

شكراً لمتابعتكم.

بخصوص موضوعكم، نود إفادتكم بأن [أضف الحالة الحالية].

سنبقيكم على اطلاع بأي تحديثات.

مع أطيب التحيات،
فريق المتابعة""",
        
        "عرض": f"""السيد/السيدة {sender} المحترم/ة،

شكراً لتواصلكم وعرضكم الكريم.

سنقوم بدراسة العرض المقدم والرد عليكم في أقرب وقت.

مع أطيب التحيات،
فريق المشتريات""",
        
        "أخرى": f"""السيد/السيدة {sender} المحترم/ة،

شكراً لتواصلكم معنا.

تم استلام رسالتكم وسنقوم بالرد عليكم قريباً.

مع أطيب التحيات،
فريق خدمة العملاء"""
    }
    
    return templates.get(intent, templates["أخرى"])


# ============ LangGraph Nodes ============

async def ingest_node(state: AgentState) -> AgentState:
    """Step 1: Ingest and clean the message"""
    state["processing_step"] = "استلام"
    
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
1. النية (intent): استفسار، طلب خدمة، شكوى، متابعة، عرض، أخرى
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
    
    # Fallback to rule-based
    classification = rule_based_classify(state["raw_message"])
    state["intent"] = classification["intent"]
    state["urgency"] = classification["urgency"]
    state["sentiment"] = classification["sentiment"]
    
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
    
    # Fallback: Basic extraction
    sentences = state["raw_message"].split('.')
    state["key_points"] = [s.strip() for s in sentences[:3] if s.strip()]
    state["action_items"] = ["مراجعة الطلب", "الرد على العميل"]
    
    return state


async def draft_node(state: AgentState) -> AgentState:
    """Step 4: Draft a response"""
    state["processing_step"] = "صياغة"
    
    sender = state.get("sender_name", "العميل الكريم")
    intent = state.get("intent", "أخرى")
    key_points = state.get("key_points", [])
    
    # Try LLM for natural, personalized Arabic response
    history_block = ""
    if state.get("conversation_history"):
        history_block = f"\nسياق المحادثة السابقة مع هذا العميل (من الأحدث إلى الأقدم):\n{state['conversation_history']}\n"

    prompt = f"""أنت موظف خدمة عملاء محترف في شركة عربية.
اكتب رداً بشرياً طبيعياً باللغة العربية الفصحى المبسّطة (ليست رسمية جداً ولا عامية).

المطلوب من الرد:
- أن يكون موجهاً مباشرة إلى العميل ({sender}) إن أمكن ذكر الاسم.
- أن يوضح أنك قرأت الرسالة وفهمت مضمونها (باختصار).
- أن يقدم معلومات أو خطوات واضحة ومحددة.
- أن يكون مشجعاً ولطيفاً، بدون مبالغة في المجاملات أو الجمل المتكررة.
- الطول المتوقع: من 3 إلى 6 أسطر كحد أقصى.

نوع الرسالة (نية العميل): {intent}
النقاط الرئيسية المستخرجة: {', '.join(key_points) or 'لم يتم استخراج نقاط واضحة'}
{history_block}
نص رسالة العميل الحالية:
{state['raw_message']}

اكتب الرد فقط بدون أي شرح إضافي أو تعداد نقطي."""

    llm_response = await call_llm(
        prompt,
        system=build_system_prompt(state.get("preferences")),
        json_mode=False,
        max_tokens=400,
    )
    
    if llm_response and len(llm_response.strip()) > 40:
        state["draft_response"] = llm_response.strip()
    else:
        # Use template-based response
        state["draft_response"] = generate_rule_based_response(state)
    
    # Generate summary
    state["summary"] = f"رسالة {intent} من {sender}. المشاعر: {state.get('sentiment', 'محايد')}. الأهمية: {state.get('urgency', 'عادي')}."
    
    # Suggested actions based on intent
    actions_map = {
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
    workflow.set_entry_point("ingest")
    workflow.add_edge("ingest", "classify")
    workflow.add_edge("classify", "extract")
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
    preferences: Optional[Dict[str, Any]] = None,
    conversation_history: Optional[str] = None,
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
        "conversation_history": conversation_history,
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

