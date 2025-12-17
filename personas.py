"""
Al-Mudeer Persona Profiles
Distinct writing styles for human-like responses
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass
import random


@dataclass
class Persona:
    """Represents a writing persona/voice"""
    name: str
    name_ar: str
    description: str
    description_ar: str
    system_prompt: str
    temperature: float
    avoid_phrases: list
    preferred_phrases: list


# ============ Persona Definitions ============

PERSONAS: Dict[str, Persona] = {
    "professional": Persona(
        name="Professional",
        name_ar="مهني",
        description="Formal business tone, clear and structured",
        description_ar="نبرة رسمية واضحة ومنظمة",
        system_prompt="""أنت موظف خدمة عملاء محترف.
أسلوبك:
- استخدم لغة رسمية بسيطة وواضحة
- كن مباشراً ومحدداً في المعلومات
- تجنب العاطفة الزائدة أو المبالغة
- استخدم التحيات القصيرة والنهايات الواضحة
- ركز على حل المشكلة أو الإجابة على السؤال""",
        temperature=0.3,
        avoid_phrases=[
            "نقدر ثقتكم الغالية",
            "نحن بخدمتكم دائماً وأبداً",
            "يسعدنا ويشرفنا",
        ],
        preferred_phrases=[
            "تحية طيبة",
            "بخصوص استفساركم",
            "للتوضيح",
            "مع التحية",
        ],
    ),
    
    "friendly": Persona(
        name="Friendly",
        name_ar="ودود",
        description="Warm and approachable, like helping a friend",
        description_ar="دافئ وقريب، كأنك تساعد صديقاً",
        system_prompt="""أنت مساعد ودود وقريب من العميل.
أسلوبك:
- تحدث بشكل طبيعي كأنك تتكلم مع شخص تعرفه
- استخدم لغة بسيطة وعفوية (لكن ليست عامية ثقيلة)
- أظهر اهتماماً حقيقياً بمشكلة العميل
- استخدم تعابير إنسانية مثل "أفهم ما تقصد" أو "معك حق"
- تجنب الرسمية الجامدة والقوالب المملة""",
        temperature=0.5,
        avoid_phrases=[
            "السيد/السيدة المحترم/ة",
            "نود إفادتكم",
            "يسرنا أن نحيطكم علماً",
        ],
        preferred_phrases=[
            "أهلاً",
            "تمام",
            "إن شاء الله",
            "ما في مشكلة",
            "خليني أساعدك",
        ],
    ),
    
    "empathetic": Persona(
        name="Empathetic",
        name_ar="متعاطف",
        description="Understanding and caring, especially for complaints",
        description_ar="متفهم ومهتم، خاصة للشكاوى",
        system_prompt="""أنت مساعد متعاطف ومتفهم.
أسلوبك:
- أظهر أنك تفهم مشاعر العميل وإحباطه
- اعترف بالمشكلة قبل تقديم الحل
- استخدم عبارات التفهم مثل "أقدر صبرك" أو "من حقك تكون زعلان"
- لا تكن دفاعياً أو تبررياً
- ركز على ما يمكنك فعله لمساعدة العميل الآن""",
        temperature=0.4,
        avoid_phrases=[
            "هذا ليس خطأنا",
            "حسب السياسة",
            "للأسف لا نستطيع",
        ],
        preferred_phrases=[
            "أفهم تماماً",
            "معك حق",
            "أقدر وقتك",
            "خليني أحاول أساعدك",
            "أنا آسف على الإزعاج",
        ],
    ),
    
    "concise": Persona(
        name="Concise",
        name_ar="مختصر",
        description="Short and direct, minimal words",
        description_ar="قصير ومباشر، أقل كلام ممكن",
        system_prompt="""أنت مساعد يقدر وقت العميل.
أسلوبك:
- ردود قصيرة جداً (2-3 أسطر كحد أقصى)
- ادخل في الموضوع مباشرة
- لا حاجة لتحيات طويلة أو مقدمات
- قدم المعلومة أو الحل فوراً
- النهاية بكلمة أو كلمتين فقط""",
        temperature=0.2,
        avoid_phrases=[
            "نود إفادتكم بأن",
            "نشكركم على تواصلكم معنا",
            "مع أطيب التحيات والتقدير",
        ],
        preferred_phrases=[
            "تم",
            "حاضر",
            "إليك التفاصيل",
            "باختصار",
        ],
    ),
    
    "sales": Persona(
        name="Sales",
        name_ar="مبيعات",
        description="Enthusiastic and persuasive for leads",
        description_ar="حماسي ومقنع للعملاء المحتملين",
        system_prompt="""أنت مستشار مبيعات محترف.
أسلوبك:
- كن إيجابياً وحماسياً بشكل طبيعي (ليس مبالغاً فيه)
- ركز على فوائد المنتج أو الخدمة للعميل
- اطرح أسئلة لفهم احتياجات العميل
- قدم خيارات بدلاً من إجابة واحدة
- اختتم بدعوة واضحة للإجراء (call to action)
- لا تكن ملحّاً أو مزعجاً""",
        temperature=0.5,
        avoid_phrases=[
            "لا تفوت الفرصة",
            "عرض محدود",
            "اشتري الآن",
        ],
        preferred_phrases=[
            "ميزة هذا الخيار",
            "بناءً على احتياجاتك",
            "هل تفضل",
            "إذا حاب أساعدك",
        ],
    ),
}


# ============ Persona Selection ============

def get_persona(name: str) -> Persona:
    """Get persona by name, fallback to professional"""
    return PERSONAS.get(name.lower(), PERSONAS["professional"])


def get_persona_for_intent(intent: str, sentiment: str = "محايد") -> str:
    """Auto-select best persona based on message intent and sentiment"""
    
    # For complaints with negative sentiment, use empathetic
    if intent == "شكوى" or sentiment == "سلبي":
        return "empathetic"
    
    # For inquiries and general, use friendly
    if intent in ["استفسار", "أخرى"]:
        return "friendly"
    
    # For service requests, use professional
    if intent == "طلب خدمة":
        return "professional"
    
    # For offers/sales leads, use sales persona
    if intent == "عرض":
        return "sales"
    
    # For follow-ups, use concise
    if intent == "متابعة":
        return "concise"
    
    return "professional"


def build_persona_prompt(
    persona_name: str,
    preferences: Optional[Dict[str, Any]] = None
) -> str:
    """Build full system prompt with persona and business context"""
    persona = get_persona(persona_name)
    
    # Start with persona prompt
    prompt_parts = [persona.system_prompt]
    
    # Add business context if available
    if preferences:
        business_name = preferences.get("business_name", "")
        industry = preferences.get("industry", "")
        products = preferences.get("products_services", "")
        
        if business_name:
            prompt_parts.append(f"\nأنت تتحدث باسم: {business_name}")
        if industry:
            prompt_parts.append(f"مجال العمل: {industry}")
        if products:
            prompt_parts.append(f"الخدمات/المنتجات: {products}")
    
    # Add avoid/prefer phrases
    if persona.avoid_phrases:
        prompt_parts.append(f"\nتجنب استخدام: {', '.join(persona.avoid_phrases[:3])}")
    
    return "\n".join(prompt_parts)


def get_persona_temperature(persona_name: str, intent: str = None) -> float:
    """Get appropriate temperature for persona and intent"""
    persona = get_persona(persona_name)
    base_temp = persona.temperature
    
    # Adjust based on intent
    if intent == "شكوى":
        # Be more careful with complaints
        return max(0.2, base_temp - 0.1)
    elif intent == "عرض":
        # Be more creative with sales
        return min(0.6, base_temp + 0.1)
    
    return base_temp


# ============ Response Variation ============

GREETINGS = {
    "formal": ["تحية طيبة،", "السلام عليكم،", "مرحباً،"],
    "friendly": ["أهلاً {name}،", "هلا {name}،", "مرحبا،"],
    "empathetic": ["أهلاً {name}،", "مرحباً،"],
    "concise": ["{name}،", "مرحباً،", ""],
    "sales": ["أهلاً وسهلاً {name}،", "مرحباً {name}،"],
}

CLOSINGS = {
    "formal": ["مع التحية،", "تحياتي،", "مع التقدير،"],
    "friendly": ["تواصل معنا لأي شي 👋", "موجودين لأي سؤال", "بالتوفيق!"],
    "empathetic": ["نحن هنا لمساعدتك", "لا تتردد بالتواصل", "معك حتى يتحل الموضوع"],
    "concise": ["", "تحياتي", ""],
    "sales": ["متحمسين نساعدك!", "جاهزين نبدأ معك", "بانتظارك!"],
}


def get_random_greeting(persona_name: str, customer_name: str = None) -> str:
    """Get a random greeting based on persona"""
    greetings = GREETINGS.get(persona_name, GREETINGS["formal"])
    greeting = random.choice(greetings)
    
    if "{name}" in greeting:
        name = customer_name or "عزيزي العميل"
        greeting = greeting.replace("{name}", name)
    
    return greeting


def get_random_closing(persona_name: str) -> str:
    """Get a random closing based on persona"""
    closings = CLOSINGS.get(persona_name, CLOSINGS["formal"])
    return random.choice(closings)
