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
from services.knowledge_base import get_knowledge_base
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
    
    # Market-Ready Features (New)
    knowledge_facts: list  # RAG facts
    tool_calls: list  # External tool executions
    needs_human_intervention: bool  # High-priority escalation flag


async def call_llm_enhanced(
    prompt: str,
    system: str,
    temperature: float = 0.3,
    json_mode: bool = False,
    max_tokens: int = 600,
    tools: Optional[list] = None,
) -> Any:
    """Enhanced LLM call using centralized llm_generate service."""
    try:
        from services.llm_provider import llm_generate
        
        response = await llm_generate(
            prompt=prompt,
            system=system,
            json_mode=json_mode,
            max_tokens=max_tokens,
            temperature=temperature,
            tools=tools
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
    
    # Run advanced rule-based analysis (fast, reliable)
    advanced_signals = {}
    try:
        from analysis_advanced import analyze_message_advanced
        advanced_result = analyze_message_advanced(state["raw_message"])
        advanced_signals = {
            "intent": advanced_result.primary_intent,
            "urgency": advanced_result.urgency_level,
            "sentiment": advanced_result.sentiment,
            "signals": advanced_result.intent_signals,
            "urgency_score": advanced_result.urgency_score
        }
        
        # Store metadata
        state["extracted_entities"] = {**advanced_result.entities}
        state["key_points"] = advanced_result.key_points
        state["action_items"] = advanced_result.action_items
        state["sentiment_score"] = advanced_result.sentiment_score
        state["frustration_level"] = advanced_result.frustration_level
    except Exception as e:
        print(f"Advanced analysis pre-pass failed: {e}")

    # Now use LLM for final classification, guided by rule-based signals
    history_block = ""
    if state.get("conversation_history"):
        history_block = f"\nسياق المحادثة السابقة:\n{state['conversation_history']}\n"
    
    hint_block = ""
    if advanced_signals:
        hint_block = f"\nتحليل أولي (تلميحات):\n- النية المحتملة: {advanced_signals['intent']}\n- إشارات النية: {', '.join(advanced_signals['signals'])}\n- مستوى الاستعجال: {advanced_signals['urgency']} (درجة: {advanced_signals['urgency_score']})\n"

    prompt = f"""حلل الرسالة التالية وحدد التصنيفات المناسبة.
    {hint_block}
    {history_block}
    
    الفئات المتاحة للنية (intent): استفسار، طلب خدمة، شكوى، متابعة، عرض، تسويق، آلي، أخرى
    
    النص:
    {state['raw_message']}
    
    أرجع JSON فقط:
    {{
        "intent": "...", 
        "urgency": "عاجل/عادي/منخفض", 
        "sentiment": "إيجابي/محايد/سلبي", 
        "language": "ar/en", 
        "dialect": "شامي/خليجي/مصري/فصحى/أخرى",
        "reasoning": "سبب اختيار هذا التصنيف"
    }}"""

    llm_response = await call_llm_enhanced(
        prompt, "أنت محلل نصوص خبير لنظام خدمة عملاء ذكي.", temperature=0.1, json_mode=True
    )
    
    if llm_response:
        try:
            classification = json_repair.loads(llm_response.content)
            state["intent"] = classification.get("intent", advanced_signals.get("intent", "أخرى"))
            state["urgency"] = classification.get("urgency", advanced_signals.get("urgency", "عادي"))
            state["sentiment"] = classification.get("sentiment", advanced_signals.get("sentiment", "محايد"))
            state["language"] = classification.get("language", "ar")
            state["dialect"] = classification.get("dialect", "فصحى")
        except Exception as e:
            print(f"LLM Classification parsing failed: {e}")
            # Fallback to advanced labels if LLM fails
            if advanced_signals:
                state["intent"] = advanced_signals["intent"]
                state["urgency"] = advanced_signals["urgency"]
                state["sentiment"] = advanced_signals["sentiment"]
    elif advanced_signals:
        # Fallback to advanced labels
        state["intent"] = advanced_signals["intent"]
        state["urgency"] = advanced_signals["urgency"]
        state["sentiment"] = advanced_signals["sentiment"]
    
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
            extracted = json_repair.loads(llm_response.content)
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
    
    # Knowledge and tools context
    knowledge_block = ""
    if state.get("knowledge_facts"):
        facts = "\n".join(state["knowledge_facts"])
        knowledge_block = f"\n=== KNOWLEDGE BASE FACTS (USE THESE TO BE ACCURATE) ===\n{facts}\n"
        
    tool_block = ""
    if state.get("tool_calls"):
        tools = json.dumps(state["tool_calls"], ensure_ascii=False)
        tool_block = f"\n=== LIVE TOOL RESULTS ===\n{tools}\n"
    
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
{knowledge_block}
{tool_block}

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
{knowledge_block.replace('KNOWLEDGE BASE FACTS', 'حقائق من قاعدة المعرفة').replace('LIVE TOOL RESULTS', 'نتائج الأدوات المباشرة')}
{tool_block.replace('LIVE TOOL RESULTS', 'نتائج الأدوات المباشرة')}

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
    
    if llm_response and llm_response.content and len(llm_response.content) > 40:
        # Post-process: remove any remaining robotic phrases
        draft = remove_robotic_phrases(llm_response.content)
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


async def enhanced_verify_node(state: EnhancedAgentState) -> EnhancedAgentState:
    """Actor-Critic Node: Verify the draft response for hallucinations or quality issues"""
    state["processing_step"] = "تدقيق"
    
    draft = state.get("draft_response", "")
    if not draft or draft.startswith("⏳"):
        return state
        
    entities = state.get("extracted_entities", {})
    preferences = state.get("preferences", {})
    
    # Context for the critic
    context = f"""
    FACTS EXTRACTED FROM MESSAGE:
    {json.dumps(entities, ensure_ascii=False)}
    
    BUSINESS PREFERENCES:
    {json.dumps(preferences, ensure_ascii=False)}
    
    DRAFT RESPONSE TO VERIFY:
    {draft}
    """
    
    prompt = f"""You are an AI Critic. Your job is to ensure the customer response is accurate, professional, and free of hallucinations.
    
    Compare the DRAFT RESPONSE with the FACTS and BUSINESS PREFERENCES.
    
    Check for:
    1. HALLUCINATIONS: Does the response mention prices, dates, or facts not in the context?
    2. UNPROFESSIONAL TONE: Is it too robotic or rude?
    3. MISSING INFO: Did the customer ask something that was ignored?
    
    Output JSON ONLY:
    {{
        "is_valid": true/false,
        "score": 0-100,
        "reason": "Explain why it failed if invalid",
        "critic_feedback": "Instructions for the AI to fix the response if invalid"
    }}
    
    {context}
    """
    
    llm_response = await call_llm_enhanced(
        prompt,
        "You are a strict quality control auditor for Arabic customer service.",
        temperature=0.1,
        json_mode=True
    )
    
    if llm_response:
        try:
            verification = json_repair.loads(llm_response.content)
            state["response_quality_score"] = verification.get("score", state["response_quality_score"])
            
            if not verification.get("is_valid", True) and verification.get("score", 100) < 70:
                state["response_quality_issues"].append(verification.get("reason", "Hallucination detected"))
                state["error"] = f"Verification failed: {verification.get('reason')}"
                # Add feedback for regeneration
                state["summary"] += f" (Verification failed: {verification.get('reason')})"
                print(f"Critic rejected response: {verification.get('reason')}")
            else:
                state["error"] = None
        except Exception as e:
            print(f"Verification parsing failed: {e}")
            
    return state

async def retrieve_knowledge_node(state: EnhancedAgentState) -> EnhancedAgentState:
    """Step 2b: Retrieve relevant business facts (RAG)"""
    state["processing_step"] = "بحث المعرفة"
    
    try:
        kb = get_knowledge_base()
        query = state["raw_message"]
        
        # Search Knowledge Base
        results = await kb.search(query, k=3)
        
        # Filter and store high-quality facts
        facts = []
        for res in results:
            if res.get("score", 1.0) < 0.5: # Lower distance = better match
                facts.append(res["text"])
        
        state["knowledge_facts"] = facts
        if facts:
            print(f"RAG: Found {len(facts)} relevant facts.")
        else:
            state["knowledge_facts"] = []
        
    except Exception as e:
        print(f"Knowledge retrieval error: {e}")
        state["knowledge_facts"] = []
        
    return state


async def tool_node(state: EnhancedAgentState) -> EnhancedAgentState:
    """Step 2c: Execute actionable tools if needed"""
    state["processing_step"] = "أدوات"
    
    intent = state.get("intent", "")
    if intent not in ["استفسار", "طلب", "طلب خدمة", "info", "order"]:
        return state

    try:
        from tools.business_tools import BUSINESS_TOOLS, execute_tool
        
        # Call LLM specifically for tools
        prompt = f"العميل يسأل: {state['raw_message']}\n\nحدد إذا كان هناك أية أدوات تحتاج لاستدعائها لتوفير معلومات دقيقة."
        response = await call_llm_enhanced(
            prompt, 
            "أنت مساعد مفوض لاستخدام الأدوات المتاحة فقط.",
            tools=BUSINESS_TOOLS,
            temperature=0
        )
        
        # Check for tool calls in response
        if response and hasattr(response, 'tool_calls') and response.tool_calls:
            results = []
            for tc in response.tool_calls:
                print(f"Executing Tool: {tc.name} with {tc.args}")
                tool_res = await execute_tool(tc.name, tc.args)
                results.append({
                    "tool": tc.name,
                    "result": tool_res
                })
            state["tool_calls"] = results
            print(f"Tool execution results: {len(results)} success.")
        
    except Exception as e:
        print(f"Tool execution error: {e}")
        
    return state


# ============ Build Enhanced Graph ============

def create_enhanced_agent():
    """Create the enhanced InboxCRM agent"""
    workflow = StateGraph(EnhancedAgentState)
    
    workflow.add_node("classify", enhanced_classify_node)
    workflow.add_node("extract", enhanced_extract_node)
    workflow.add_node("retrieve", retrieve_knowledge_node)
    workflow.add_node("tool", tool_node)
    workflow.add_node("draft", enhanced_draft_node)
    workflow.add_node("verify", enhanced_verify_node)
    
    # Routing logic
    def route_enhanced(state: EnhancedAgentState):
        intent = state.get("intent", "أخرى")
        if intent in ["تسويق", "آلي", "spam", "marketing", "automated"]:
            return "end"
        return "extract"

    def route_after_verify(state: EnhancedAgentState):
        """Loop back if quality is too low, or flag for human intervention"""
        
        # Sentiment-based Escalation Logic
        sentiment = state.get("sentiment", "محايد")
        urgency = state.get("urgency", "عادي")
        quality_score = state.get("response_quality_score", 100)
        
        # Market-Ready Rule: Escalate if angry and urgent, or if quality is consistently low
        if (sentiment == "سلبي" and urgency == "عاجل") or quality_score < 60:
            state["needs_human_intervention"] = True
            print(f"MARKET-READY: Escalating to human (Sentiment: {sentiment}, Quality: {quality_score})")
            
        if state.get("error") and "Verification failed" in state["error"]:
             # In production we'd use a loop counter via state
             return "end" 
        return "end"

    workflow.set_entry_point("classify")
    workflow.add_conditional_edges(
        "classify",
        route_enhanced,
        {
            "extract": "extract",
            "end": END
        }
    )
    workflow.add_edge("extract", "retrieve")
    workflow.add_edge("retrieve", "tool")
    workflow.add_edge("tool", "draft")
    workflow.add_edge("draft", "verify")
    workflow.add_conditional_edges(
        "verify",
        route_after_verify,
        {
            "end": END
        }
    )
    
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
        "knowledge_facts": [],
        "tool_calls": [],
        "needs_human_intervention": False,
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
