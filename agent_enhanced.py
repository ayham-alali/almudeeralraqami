"""
Al-Mudeer Enhanced AI Agent
Human-like responses with persona support, anti-robotic patterns, and style learning
"""

import json
import json_repair
import re
from typing import TypedDict, Optional, Dict, Any
import os

from langgraph.graph import StateGraph, END

from personas import (
    get_persona,
    get_persona_for_intent,
    build_persona_prompt,
    get_persona_temperature,
    get_random_greeting,
    get_random_closing,
)
from humanize import (
    build_few_shot_prompt,
    remove_robotic_phrases,
    get_dynamic_temperature,
    check_response_quality,
    ROBOTIC_PHRASES,
)
from models import update_daily_analytics
import asyncio
from message_filters import apply_filters



# Note: LLM configuration is centralized in services/llm_provider.py
# This file uses llm_generate() which handles OpenAI/Gemini failover


class EnhancedAgentState(TypedDict):
    """Enhanced state with persona, style learning, and quality tracking"""
    # Input
    raw_message: str
    message_type: str
    
    # Classification
    intent: str
    urgency: str
    sentiment: str
    language: Optional[str]
    dialect: Optional[str]
    
    # Extraction
    sender_name: Optional[str]
    sender_contact: Optional[str]
    key_points: list
    action_items: list
    extracted_entities: dict
    
    # Customer context
    customer_history: Optional[Dict[str, Any]]
    relationship_level: str  # new, returning, vip
    
    # Persona
    persona_name: str
    persona_auto_selected: bool
    
    # Style Learning (new)
    use_learned_style: bool  # Whether to use learned style
    style_profile: Optional[Dict[str, Any]]  # Learned style profile
    
    # Output
    summary: str
    draft_response: str
    suggested_actions: list
    
    # Quality
    response_quality_score: int
    response_quality_issues: list
    
    # Metadata
    error: Optional[str]
    processing_step: str
    preferences: Optional[Dict[str, Any]]
    conversation_history: Optional[str]


async def call_llm_enhanced(
    prompt: str,
    system: str,
    temperature: float = 0.3,
    json_mode: bool = False,
    max_tokens: int = 600,
) -> Optional[str]:
    """Enhanced LLM call using centralized llm_generate service.
    
    Uses the centralized LLM provider which supports:
    - OpenAI and Gemini with automatic failover
    - Rate limiting and retry logic
    - Response caching
    """
    try:
        from services.llm_provider import llm_generate
        
        response = await llm_generate(
            prompt=prompt,
            system=system,
            json_mode=json_mode,
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        return response
    except Exception as e:
        print(f"LLM call failed: {e}")
        return None


# ============ Enhanced Pipeline Nodes ============

async def enhanced_classify_node(state: EnhancedAgentState) -> EnhancedAgentState:
    """Classify with advanced analysis and dialect awareness"""
    state["processing_step"] = "تصنيف"
    
    # Update analytics (received)
    if state.get("preferences") and state["preferences"].get("license_key_id"):
        try:
            asyncio.create_task(update_daily_analytics(
                license_id=state["preferences"]["license_key_id"],
                messages_received=1
            ))
        except Exception as e:
            print(f"Analytics update failed: {e}")
    
    # First, run advanced rule-based analysis (fast, reliable)
    try:
        from analysis_advanced import analyze_message_advanced, analysis_to_dict
        
        advanced_result = analyze_message_advanced(state["raw_message"])
        
        # Use advanced analysis results
        state["intent"] = advanced_result.primary_intent
        state["urgency"] = advanced_result.urgency_level
        state["sentiment"] = advanced_result.sentiment
        state["language"] = advanced_result.language
        state["dialect"] = advanced_result.dialect
        
        # Store additional analysis data in extracted_entities for now
        state["extracted_entities"] = {
            **advanced_result.entities,
            "_analysis": {
                "intent_confidence": advanced_result.intent_confidence,
                "intent_signals": advanced_result.intent_signals,
                "urgency_score": advanced_result.urgency_score,
                "urgency_signals": advanced_result.urgency_signals,
                "has_deadline": advanced_result.has_deadline,
                "deadline": advanced_result.deadline_text,
                "sentiment_score": advanced_result.sentiment_score,
                "frustration_level": advanced_result.frustration_level,
                "emotional_cues": advanced_result.emotional_cues,
                "formality": advanced_result.formality_level,
                "questions": advanced_result.questions_asked,
            }
        }
        
        # Set key points and actions from advanced analysis
        state["key_points"] = advanced_result.key_points
        state["action_items"] = advanced_result.action_items
        
    except Exception as e:
        print(f"Advanced analysis failed, using LLM fallback: {e}")
        
        # Fallback to LLM-based classification
        history_block = ""
        if state.get("conversation_history"):
            history_block = f"\nسياق المحادثة السابقة:\n{state['conversation_history']}\n"
        
        prompt = f"""حلل الرسالة التالية:
1. النية (intent): استفسار، طلب خدمة، شكوى، متابعة، عرض، تسويق، آلي، أخرى
2. الأهمية (urgency): عاجل، عادي، منخفض
3. المشاعر (sentiment): إيجابي، محايد، سلبي
4. اللغة: ar, en, أو أخرى
5. اللهجة: شامي، خليجي، مصري، فصحى، أخرى
{history_block}
النص:
{state['raw_message']}

أرجع JSON فقط:
{{"intent": "...", "urgency": "...", "sentiment": "...", "language": "...", "dialect": "..."}}"""

        llm_response = await call_llm_enhanced(
            prompt, "أنت محلل نصوص خبير.", temperature=0.2, json_mode=True
        )
        
        if llm_response:
            try:
                classification = json_repair.loads(llm_response)
                state["intent"] = classification.get("intent", "أخرى")
                state["urgency"] = classification.get("urgency", "عادي")
                state["sentiment"] = classification.get("sentiment", "محايد")
                state["language"] = classification.get("language", "ar")
                state["dialect"] = classification.get("dialect")
            except json.JSONDecodeError:
                state["intent"] = "أخرى"
                state["urgency"] = "عادي"
                state["sentiment"] = "محايد"
        else:
            state["intent"] = "أخرى"
            state["urgency"] = "عادي"
            state["sentiment"] = "محايد"
    
    # Customer relationship context
    if state.get("customer_history"):
        ch = state["customer_history"]
        order_count = ch.get("order_count", 0)
        if order_count > 5:
            state["relationship_level"] = "vip"
        elif order_count > 0:
            state["relationship_level"] = "returning"
        else:
            state["relationship_level"] = "new"
    
    # Auto-select persona based on intent/sentiment
    if not state.get("persona_name"):
        state["persona_name"] = get_persona_for_intent(
            state["intent"], state["sentiment"]
        )
        state["persona_auto_selected"] = True
    
    return state


async def enhanced_extract_node(state: EnhancedAgentState) -> EnhancedAgentState:
    """Extract with enhanced entity recognition"""
    state["processing_step"] = "استخراج"
    
    # Regex-based extraction (reliable)
    message = state["raw_message"]
    entities = {}
    
    # Phone patterns
    phones = re.findall(r'(?:\+|00)?(?:963|966|971|962|961|20|965|974)\d{8,10}', message)
    if phones:
        entities["phones"] = list(set(phones))
    
    # Email
    emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', message)
    if emails:
        entities["emails"] = emails
        state["sender_contact"] = emails[0]
    
    # Dates
    dates = re.findall(r'\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}', message)
    if dates:
        entities["dates"] = dates
    
    # Money
    amounts = re.findall(r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:ل\.س|ليرة|دولار|\$|ر\.س)', message)
    if amounts:
        entities["amounts"] = amounts
    
    # Names
    name_match = re.search(r'(?:السيد|السيدة|الأستاذ|أخي|أختي)\s+([\u0600-\u06FF\s]+)', message)
    if name_match:
        entities["mentioned_name"] = name_match.group(1).strip()
        if not state.get("sender_name"):
            state["sender_name"] = entities["mentioned_name"]
    
    state["extracted_entities"] = entities
    
    # LLM extraction for key points
    prompt = f"""استخرج من الرسالة:
1. النقاط الرئيسية (3 كحد أقصى)
2. الإجراءات المطلوبة

الرسالة:
{message}

JSON فقط:
{{"key_points": ["..."], "action_items": ["..."]}}"""

    llm_response = await call_llm_enhanced(
        prompt,
        "أنت مستخرج معلومات دقيق.",
        temperature=0.2,
        json_mode=True
    )
    
    if llm_response:
        try:
            extracted = json_repair.loads(llm_response)
            state["key_points"] = extracted.get("key_points", [])
            state["action_items"] = extracted.get("action_items", [])
        except:
            state["key_points"] = []
            state["action_items"] = ["مراجعة الرسالة"]
    
    return state


async def enhanced_draft_node(state: EnhancedAgentState) -> EnhancedAgentState:
    """Generate human-like draft with persona, style learning, and anti-robotic patterns"""
    state["processing_step"] = "صياغة"
    
    persona_name = state.get("persona_name", "professional")
    persona = get_persona(persona_name)
    sender = state.get("sender_name", "عزيزي العميل")
    intent = state.get("intent", "أخرى")
    sentiment = state.get("sentiment", "محايد")
    key_points = state.get("key_points", [])
    dialect = state.get("dialect", "فصحى")
    
    # Build persona-aware system prompt
    system_prompt = build_persona_prompt(
        persona_name,
        state.get("preferences")
    )
    
    # Add learned style instructions if enabled
    style_instructions = ""
    if state.get("use_learned_style") and state.get("style_profile"):
        try:
            from style_learning import StyleProfile
            profile = StyleProfile.from_dict(state["style_profile"])
            style_instructions = f"""

=== أسلوب الكتابة المتعلم من رسائلك السابقة ===
{profile.to_prompt()}
===

استخدم هذا الأسلوب في كتابة الرد."""
        except Exception:
            pass
    
    # Get detected language
    language = state.get("language", "ar")
    
    # Get dynamic temperature
    temperature = get_dynamic_temperature(
        intent, sentiment, persona.temperature
    )
    
    # Build few-shot example
    few_shot = build_few_shot_prompt(intent)
    
    # Customer relationship context
    relationship_context = ""
    if state.get("relationship_level") == "vip":
        relationship_context = "\nThis is a VIP customer - show special appreciation."
    elif state.get("relationship_level") == "returning":
        relationship_context = "\nThis is a returning customer - you can acknowledge that."
    
    # Build language-specific prompt
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
        
        history_block = ""
        if state.get("conversation_history"):
            history_block = f"\nPREVIOUS CONVERSATION CONTEXT:\n{state['conversation_history']}\n"

        prompt = f"""{few_shot}

🗣️ IMPORTANT: Respond in {lang_name} (same language as customer)!

{history_block}

Write a response to the customer ({sender}) based on:
- Message type: {intent}
- Sentiment: {sentiment}
- Language: {lang_name}
- Key points: {', '.join(key_points) or 'Not specified'}
{relationship_context}
{style_instructions}

Customer's message:
{state['raw_message']}

⚠️ Very important: Match the customer's language! Respond in {lang_name}.

Write only the response in {lang_name} (3-6 lines), no explanation:"""

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
        
        # Anti-robotic instructions
        anti_robotic = f"""
تجنب هذه العبارات النمطية:
{', '.join(ROBOTIC_PHRASES[:5])}

بدلاً منها، استخدم لغة طبيعية وعفوية."""
        
        history_block = ""
        if state.get("conversation_history"):
            history_block = f"\nسياق المحادثة السابقة:\n{state['conversation_history']}\n"

        prompt = f"""{few_shot}

🗣️ اللهجة المطلوبة: {dialect}
{dialect_instruction if dialect_instruction else "استخدم عربية فصحى مبسّطة وسهلة الفهم."}

{history_block}

اكتب رداً للعميل ({sender}) بناءً على:
- نوع الرسالة: {intent}
- المشاعر: {sentiment}
- اللهجة المكتشفة: {dialect}
- النقاط الرئيسية: {', '.join(key_points) or 'غير محددة'}
{relationship_context}
{style_instructions}

رسالة العميل:
{state['raw_message']}
{anti_robotic}

⚠️ مهم جداً: طابق لهجة العميل في ردك! إذا كتب بالسعودي، رد بالسعودي. إذا كتب بالمصري، رد بالمصري.

اكتب الرد فقط بنفس لهجة العميل (3-6 أسطر)، بدون شرح:"""

    llm_response = await call_llm_enhanced(
        prompt,
        system_prompt,
        temperature=temperature,
        max_tokens=400,
    )
    
    if llm_response and len(llm_response) > 40:
        # Post-process: remove any remaining robotic phrases
        draft = remove_robotic_phrases(llm_response)
        state["draft_response"] = draft
    else:
        # Fallback with persona-aware greeting/closing
        greeting = get_random_greeting(persona_name, sender)
        closing = get_random_closing(persona_name)
        
        state["draft_response"] = f"""{greeting}

وصلتني رسالتك وسأتابع الموضوع.

{closing}"""
    
    # Generate summary
    state["summary"] = f"رسالة {intent} من {sender}. المشاعر: {sentiment}. اللهجة: {dialect}."
    
    # Check response quality
    quality = check_response_quality(state["draft_response"])
    state["response_quality_score"] = quality["score"]
    state["response_quality_issues"] = quality["issues"]
    
    # Suggested actions
    actions_map = {
        "استفسار": ["الرد", "إضافة للأسئلة الشائعة"],
        "طلب خدمة": ["إنشاء طلب", "تحديد موعد"],
        "شكوى": ["تصعيد", "فتح تذكرة", "اتصال"],
        "متابعة": ["تحديث الحالة"],
        "عرض": ["دراسة العرض"],
        "أخرى": ["مراجعة يدوية"],
    }
    state["suggested_actions"] = actions_map.get(intent, ["مراجعة"])
    
    # Update analytics (replied)
    if state.get("preferences") and state["preferences"].get("license_key_id"):
        try:
            asyncio.create_task(update_daily_analytics(
                license_id=state["preferences"]["license_key_id"],
                messages_replied=1,
                sentiment=state.get("sentiment", "محايد"),
                time_saved_seconds=180 # 3 minutes per AI response
            ))
        except Exception as e:
            print(f"Analytics reply update failed: {e}")

    return state


# ============ Build Enhanced Graph ============

def create_enhanced_agent():
    """Create the enhanced InboxCRM agent"""
    workflow = StateGraph(EnhancedAgentState)
    
    workflow.add_node("classify", enhanced_classify_node)
    workflow.add_node("extract", enhanced_extract_node)
    workflow.add_node("draft", enhanced_draft_node)
    
    # Routing logic
    def route_enhanced(state: EnhancedAgentState):
        intent = state.get("intent", "أخرى")
        if intent in ["تسويق", "آلي", "spam", "marketing", "automated"]:
            return "end"
        return "extract"

    workflow.set_entry_point("classify")
    workflow.add_conditional_edges(
        "classify",
        route_enhanced,
        {
            "extract": "extract",
            "end": END
        }
    )
    workflow.add_edge("extract", "draft")
    workflow.add_edge("draft", END)
    
    return workflow.compile()


# Singleton
_enhanced_agent = None


def get_enhanced_agent():
    """Get or create the enhanced agent instance"""
    global _enhanced_agent
    if _enhanced_agent is None:
        _enhanced_agent = create_enhanced_agent()
    return _enhanced_agent


async def process_message_enhanced(
    message: str,
    message_type: str = None,
    sender_name: str = None,
    sender_contact: str = None,
    preferences: Optional[Dict[str, Any]] = None,
    conversation_history: Optional[str] = None,
    customer_history: Optional[Dict[str, Any]] = None,
    persona_name: str = None,
    # Style learning options
    use_learned_style: bool = False,
    style_profile: Optional[Dict[str, Any]] = None,
) -> dict:
    """
    Process a message with enhanced human-like responses.
    
    Args:
        message: The raw message text
        message_type: Type of message (email, telegram, whatsapp, general)
        sender_name: Customer's name
        sender_contact: Customer's email or phone
        preferences: Business preferences (tone, business_name, etc.)
        conversation_history: Previous conversation with this customer
        customer_history: Customer data (order_count, etc.)
        persona_name: Specific persona to use (professional, friendly, etc.)
        use_learned_style: If True, use learned style from user's past messages
        style_profile: The StyleProfile dict (from analyze_messages_for_style)
    """
    
    agent = get_enhanced_agent()
    
    # --- Step 0: Local Blocking (Smart Filtering) ---
    should_process, reason = await apply_filters(
        message={"body": message, "sender_contact": sender_contact},
        license_id=preferences.get("license_key_id", 0) if preferences else 0,
        recent_messages=None
    )
    
    if not should_process:
        print(f"Enhanced Agent: Message filtered locally: {reason}")
        return {
            "success": True,
            "data": {
                "intent": "آلي" if "Automated" in reason else "ignored",
                "urgency": "منخفض",
                "sentiment": "محايد",
                "summary": f"تم تجاهل الرسالة: {reason}",
                "draft_response": "", 
                "processing_notes": f"Filtered by: {reason}",
                # Fill required enhanced fields with dummies
                "persona_used": "none",
                "persona_auto_selected": False,
                "relationship_level": "new",
                "key_points": [],
                "action_items": [],
                "extracted_entities": {},
                "suggested_actions": [],
                "message_type": message_type or "general",
                "language": "ar",
                "dialect": None,
                "sender_name": sender_name,
                "sender_contact": sender_contact,
                "quality_score": 0,
                "quality_issues": []
            }
        }

    initial_state: EnhancedAgentState = {

        "raw_message": message,
        "message_type": message_type or "general",
        "intent": "",
        "urgency": "",
        "sentiment": "",
        "language": None,
        "dialect": None,
        "sender_name": sender_name,
        "sender_contact": sender_contact,
        "key_points": [],
        "action_items": [],
        "extracted_entities": {},
        "customer_history": customer_history,
        "relationship_level": "new",
        "persona_name": persona_name or "",
        "persona_auto_selected": False,
        "use_learned_style": use_learned_style,
        "style_profile": style_profile,
        "summary": "",
        "draft_response": "",
        "suggested_actions": [],
        "response_quality_score": 0,
        "response_quality_issues": [],
        "error": None,
        "processing_step": "",
        "preferences": preferences,
        "conversation_history": conversation_history,
    }
    
    try:
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
                "message_type": final_state["message_type"],
                # New fields
                "persona_used": final_state["persona_name"],
                "persona_auto_selected": final_state["persona_auto_selected"],
                "relationship_level": final_state["relationship_level"],
                "quality_score": final_state["response_quality_score"],
                "quality_issues": final_state["response_quality_issues"],
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"حدث خطأ: {str(e)}"
        }
