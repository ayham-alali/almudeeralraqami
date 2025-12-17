"""
Al-Mudeer Humanization Utilities
Anti-robotic patterns and natural language helpers
"""

from typing import List, Dict, Optional
import random
import re


# ============ Anti-Robotic Patterns ============

# Phrases to AVOID (robotic, overused, templated)
ROBOTIC_PHRASES = [
    # Overly formal greetings
    "السيد/السيدة المحترم/ة",
    "نود إفادتكم",
    "يسرنا أن نحيطكم علماً",
    "نقدر ثقتكم الغالية بنا",
    "نحن بخدمتكم دائماً وأبداً",
    "يسعدنا ويشرفنا التواصل معكم",
    
    # Repetitive closings
    "مع أطيب التحيات والتقدير والاحترام",
    "ونحن في انتظار ردكم الكريم",
    "لا تترددوا في التواصل معنا في أي وقت",
    
    # Corporate jargon
    "وفقاً للسياسات المعتمدة",
    "نلتزم بتقديم أفضل الخدمات",
    "نسعى جاهدين لتحقيق رضاكم",
    "نحرص على تلبية كافة احتياجاتكم",
    
    # Filler words
    "وبالتالي",
    "وعليه",
    "وفي هذا السياق",
]


# Phrases to USE (natural, human-like)
NATURAL_PHRASES = {
    "acknowledgment": [
        "فهمت طلبك",
        "واضح",
        "تمام، شفت رسالتك",
        "وصلتني رسالتك",
    ],
    "apology": [
        "آسف على الإزعاج",
        "معك حق",
        "أفهم إحباطك",
        "ما كان لازم يصير كذا",
    ],
    "confirmation": [
        "أكيد",
        "طبعاً",
        "إن شاء الله",
        "حاضر",
    ],
    "closing": [
        "موجودين لأي سؤال",
        "تواصل معنا متى ما احتجت",
        "بالخدمة",
        "أي شي ثاني؟",
    ],
    "transition": [
        "بخصوص",
        "عن سؤالك",
        "بالنسبة لـ",
        "أما عن",
    ],
}


# ============ Few-Shot Examples ============

# Real examples of good human-like responses for each intent
FEW_SHOT_EXAMPLES = {
    "استفسار": [
        {
            "customer": "كم سعر الخدمة الشهرية؟",
            "response": """أهلاً!

الخدمة الشهرية بـ 150 دولار، وتشمل الدعم الفني والتحديثات.

إذا حاب تفاصيل أكثر أو عندك أسئلة ثانية، أنا موجود 👋"""
        },
        {
            "customer": "هل تقدمون خدمات التوصيل لدمشق؟",
            "response": """مرحباً،

نعم نوصل لدمشق وريفها. التوصيل يأخذ عادة 2-3 أيام عمل.

رسوم التوصيل 5000 ل.س للطلبات أقل من 50,000 ل.س، ومجاناً للطلبات الأكبر.

هل تحب تكمل الطلب؟"""
        },
    ],
    
    "شكوى": [
        {
            "customer": "الطلب تأخر أسبوع ولم يصل حتى الآن!",
            "response": """أهلاً،

أفهم تماماً إحباطك، أسبوع فعلاً وقت طويل ومو مقبول.

خليني أتحقق من الطلب الآن وأرجعلك بتحديث. لو في مشكلة، راح نلاقي حل مناسب إن شاء الله.

ممكن تعطيني رقم الطلب؟"""
        },
        {
            "customer": "المنتج وصل مكسور!",
            "response": """أهلاً،

آسف جداً على هالموقف، ما كان لازم يصير كذا.

الحل: راح نرسلك منتج جديد اليوم بدون أي تكلفة إضافية. بس أرسلي صورة للمنتج المكسور للتوثيق.

معك حق تكون زعلان، ونحن نتحمل المسؤولية."""
        },
    ],
    
    "طلب خدمة": [
        {
            "customer": "أريد حجز موعد يوم الأحد",
            "response": """أهلاً!

تمام، الأحد متاح. عندنا أوقات:
- 10 الصبح
- 2 الظهر  
- 5 العصر

أي وقت يناسبك؟"""
        },
    ],
    
    "متابعة": [
        {
            "customer": "شو صار بطلبي؟",
            "response": """أهلاً،

طلبك الآن عند قسم الشحن وراح يتم إرساله اليوم إن شاء الله.

راح توصلك رسالة فيها رقم التتبع بمجرد ما يتحرك."""
        },
    ],
}


# ============ Response Enhancement Functions ============

def remove_robotic_phrases(text: str) -> str:
    """Remove common robotic phrases from response"""
    result = text
    for phrase in ROBOTIC_PHRASES:
        result = result.replace(phrase, "")
    
    # Clean up extra whitespace
    result = re.sub(r'\n{3,}', '\n\n', result)
    result = re.sub(r' {2,}', ' ', result)
    
    return result.strip()


def add_natural_element(response: str, element_type: str) -> str:
    """Add a natural phrase of specified type"""
    if element_type in NATURAL_PHRASES:
        phrase = random.choice(NATURAL_PHRASES[element_type])
        return phrase
    return ""


def get_few_shot_example(intent: str) -> Optional[Dict]:
    """Get a random few-shot example for the given intent"""
    examples = FEW_SHOT_EXAMPLES.get(intent, [])
    if examples:
        return random.choice(examples)
    return None


def build_few_shot_prompt(intent: str) -> str:
    """Build few-shot prompt section for the given intent"""
    example = get_few_shot_example(intent)
    if not example:
        return ""
    
    return f"""
مثال على رد جيد:
رسالة العميل: {example['customer']}
الرد المطلوب:
{example['response']}

---
"""


# ============ Response Quality Checks ============

def check_response_quality(response: str) -> Dict[str, any]:
    """Check response quality and return issues"""
    issues = []
    suggestions = []
    score = 100
    
    # Check for robotic phrases
    for phrase in ROBOTIC_PHRASES[:10]:
        if phrase in response:
            issues.append(f"يحتوي على عبارة نمطية: {phrase}")
            score -= 10
    
    # Check length
    if len(response) > 800:
        issues.append("الرد طويل جداً")
        suggestions.append("اختصر الرد ليكون أقل من 800 حرف")
        score -= 15
    elif len(response) < 50:
        issues.append("الرد قصير جداً")
        suggestions.append("أضف تفاصيل أكثر")
        score -= 10
    
    # Check for missing greeting
    greetings = ["أهلاً", "مرحباً", "هلا", "السلام"]
    has_greeting = any(g in response[:50] for g in greetings)
    if not has_greeting:
        suggestions.append("أضف تحية في البداية")
        score -= 5
    
    # Check for all caps (shouting)
    if response.isupper():
        issues.append("الرد بحروف كبيرة (يبدو كصراخ)")
        score -= 20
    
    return {
        "score": max(0, score),
        "issues": issues,
        "suggestions": suggestions,
        "is_good": score >= 70,
    }


# ============ Dynamic Temperature ============

def get_dynamic_temperature(intent: str, sentiment: str, persona_base: float = 0.3) -> float:
    """Calculate dynamic temperature based on context"""
    temp = persona_base
    
    # Complaints need careful, consistent responses
    if intent == "شكوى":
        temp = max(0.2, temp - 0.1)
    
    # Negative sentiment needs more careful responses
    if sentiment == "سلبي":
        temp = max(0.2, temp - 0.1)
    
    # Sales inquiries can be more creative
    if intent == "عرض":
        temp = min(0.6, temp + 0.15)
    
    # General inquiries can have some variation
    if intent == "استفسار":
        temp = min(0.5, temp + 0.1)
    
    return round(temp, 2)


# ============ Anti-Repetition ============

# Track recent phrases to avoid repetition
_recent_phrases = []
MAX_RECENT = 10


def avoid_repetition(phrase: str) -> bool:
    """Check if phrase was used recently, track it if not"""
    global _recent_phrases
    
    # Normalize phrase
    normalized = phrase.strip().lower()[:50]
    
    if normalized in _recent_phrases:
        return False  # Skip this phrase
    
    # Add to recent and trim
    _recent_phrases.append(normalized)
    if len(_recent_phrases) > MAX_RECENT:
        _recent_phrases.pop(0)
    
    return True


def get_unique_greeting(persona_name: str, customer_name: str = None) -> str:
    """Get a greeting that wasn't used recently"""
    from personas import GREETINGS
    
    greetings = GREETINGS.get(persona_name, GREETINGS.get("formal", []))
    random.shuffle(greetings)
    
    for greeting in greetings:
        if "{name}" in greeting:
            name = customer_name or "عزيزي العميل"
            greeting = greeting.replace("{name}", name)
        
        if avoid_repetition(greeting):
            return greeting
    
    # Fallback if all used recently
    return greetings[0] if greetings else "مرحباً،"
