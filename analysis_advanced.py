"""
Al-Mudeer Advanced Message Analysis
Enhanced entity extraction, intent detection, and NLP for Arabic business context
"""

import re
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass
class AnalysisResult:
    """Comprehensive message analysis result"""
    
    # Intent & classification
    primary_intent: str
    secondary_intent: Optional[str]
    intent_confidence: float  # 0.0 - 1.0
    intent_signals: List[str]  # Why we detected this intent
    
    # Urgency
    urgency_level: str  # critical, high, normal, low
    urgency_score: int  # 1-10
    urgency_signals: List[str]
    has_deadline: bool
    deadline_text: Optional[str]
    
    # Sentiment
    sentiment: str  # positive, neutral, negative
    sentiment_score: float  # -1.0 to 1.0
    emotional_cues: List[str]
    frustration_level: int  # 0-10
    
    # Language
    language: str
    dialect: str
    formality_level: str  # formal, semi-formal, informal
    
    # Entities
    entities: Dict[str, Any]
    
    # Summary
    key_points: List[str]
    action_items: List[str]
    questions_asked: List[str]
    
    # Metadata
    word_count: int
    has_attachments_mentioned: bool
    is_reply: bool
    is_forwarded: bool


# ============ Intent Detection ============

INTENT_PATTERNS = {
    "استفسار_سعر": {
        "patterns": ["كم سعر", "السعر", "تكلفة", "أسعار", "كم يكلف", "بكم"],
        "weight": 1.0,
        "ar": "استفسار عن السعر",
    },
    "استفسار_توفر": {
        "patterns": ["متوفر", "عندكم", "يوجد", "متاح", "موجود", "في ستوك"],
        "weight": 0.9,
        "ar": "استفسار عن التوفر",
    },
    "استفسار_عام": {
        "patterns": ["كيف", "ما هي", "ما هو", "ممكن أعرف", "سؤال", "استفسار"],
        "weight": 0.7,
        "ar": "استفسار عام",
    },
    "طلب_خدمة": {
        "patterns": ["أريد", "أرغب", "أحتاج", "أبغى", "بدي", "نريد", "طلب"],
        "weight": 1.0,
        "ar": "طلب خدمة",
    },
    "طلب_موعد": {
        "patterns": ["موعد", "حجز", "ميعاد", "أحجز", "وقت مناسب"],
        "weight": 0.95,
        "ar": "طلب موعد",
    },
    "شكوى": {
        "patterns": ["شكوى", "مشكلة", "لم يعمل", "لا يعمل", "تأخر", "سيء", "خربان"],
        "weight": 1.0,
        "ar": "شكوى",
    },
    "شكوى_خدمة": {
        "patterns": ["خدمة سيئة", "معاملة", "لم يرد", "تجاهل", "ما في رد"],
        "weight": 0.9,
        "ar": "شكوى من الخدمة",
    },
    "شكوى_منتج": {
        "patterns": ["منتج معيب", "مكسور", "غلط", "ناقص", "تالف"],
        "weight": 0.9,
        "ar": "شكوى من المنتج",
    },
    "متابعة": {
        "patterns": ["متابعة", "بخصوص", "استكمال", "تذكير", "شو صار", "وين صار"],
        "weight": 1.0,
        "ar": "متابعة طلب",
    },
    "متابعة_طلب": {
        "patterns": ["رقم الطلب", "طلبي", "أين طلبي", "وصل الطلب"],
        "weight": 0.95,
        "ar": "متابعة طلب محدد",
    },
    "عرض_شراكة": {
        "patterns": ["عرض", "شراكة", "تعاون", "اتفاقية", "وكالة"],
        "weight": 0.8,
        "ar": "عرض شراكة",
    },
    "عرض_تسويق": {
        "patterns": ["إعلان", "تسويق", "ترويج", "حملة"],
        "weight": 0.7,
        "ar": "عرض تسويقي",
    },
    "شكر_رضا": {
        "patterns": ["شكراً", "ممتاز", "رائع", "مشكورين", "الله يعطيكم العافية"],
        "weight": 0.8,
        "ar": "شكر وتقدير",
    },
    "ردود_تلقائية": {
        "patterns": ["خارج المكتب", "إجازة", "auto-reply", "automatic"],
        "weight": 0.9,
        "ar": "رد تلقائي",
    },
    "طلب_إلغاء": {
        "patterns": ["إلغاء", "ألغي", "لا أريد", "تراجع", "رجوع"],
        "weight": 1.0,
        "ar": "طلب إلغاء",
    },
    "طلب_استرداد": {
        "patterns": ["استرداد", "ارجاع", "فلوسي", "المبلغ", "refund"],
        "weight": 1.0,
        "ar": "طلب استرداد",
    },
}


def detect_intent(message: str) -> Tuple[str, Optional[str], float, List[str]]:
    """
    Detect primary and secondary intent with confidence score.
    Returns: (primary_intent, secondary_intent, confidence, signals)
    """
    scores = {}
    signals = {}
    
    message_lower = message.lower()
    
    for intent_key, intent_data in INTENT_PATTERNS.items():
        score = 0
        found_patterns = []
        
        for pattern in intent_data["patterns"]:
            if pattern in message_lower or pattern in message:
                score += intent_data["weight"]
                found_patterns.append(pattern)
        
        if score > 0:
            scores[intent_key] = score
            signals[intent_key] = found_patterns
    
    if not scores:
        return "أخرى", None, 0.5, ["لم يتم اكتشاف نمط واضح"]
    
    # Sort by score
    sorted_intents = sorted(scores.items(), key=lambda x: -x[1])
    
    primary = sorted_intents[0][0]
    primary_score = sorted_intents[0][1]
    
    # Map to general category
    general_intent = primary.split("_")[0] if "_" in primary else primary
    
    secondary = None
    if len(sorted_intents) > 1:
        secondary = sorted_intents[1][0]
    
    # Calculate confidence
    max_possible = max(d["weight"] * len(d["patterns"]) for d in INTENT_PATTERNS.values())
    confidence = min(1.0, primary_score / (max_possible * 0.3))
    
    return general_intent, secondary, round(confidence, 2), signals.get(primary, [])


# ============ Urgency Detection ============

URGENCY_SIGNALS = {
    "critical": {
        "patterns": ["طارئ", "فوراً", "الآن", "حالاً", "قبل فوات الأوان", "مستعجل جداً"],
        "score": 10,
    },
    "high": {
        "patterns": ["عاجل", "ضروري", "اليوم", "بأسرع وقت", "مهم جداً", "لازم"],
        "score": 8,
    },
    "normal": {
        "patterns": ["متى ما ممكن", "قريباً", "لو سمحت", "إذا ممكن"],
        "score": 5,
    },
    "low": {
        "patterns": ["لاحقاً", "عندما تتوفر", "مو مستعجل", "متى ما تقدر", "بوقت فراغك"],
        "score": 2,
    },
}

DEADLINE_PATTERNS = [
    r'(?:قبل|حتى|بحلول)\s+(?:يوم\s+)?(\d{1,2}[/\-]\d{1,2}(?:[/\-]\d{2,4})?)',
    r'(?:قبل|خلال)\s+(\d+)\s*(?:يوم|ساعة|أسبوع)',
    r'(?:يوم\s+)?(?:الأحد|الاثنين|الثلاثاء|الأربعاء|الخميس|الجمعة|السبت)',
    r'(?:غداً|بكرة|بعد غد|اليوم)',
]


def detect_urgency(message: str) -> Tuple[str, int, List[str], bool, Optional[str]]:
    """
    Detect urgency level with signals and deadline.
    Returns: (level, score, signals, has_deadline, deadline_text)
    """
    found_signals = []
    max_score = 5  # Default normal
    level = "normal"
    
    message_lower = message.lower()
    
    for urgency_level, data in URGENCY_SIGNALS.items():
        for pattern in data["patterns"]:
            if pattern in message_lower or pattern in message:
                found_signals.append(pattern)
                if data["score"] > max_score:
                    max_score = data["score"]
                    level = urgency_level
    
    # Check for deadlines
    has_deadline = False
    deadline_text = None
    
    for pattern in DEADLINE_PATTERNS:
        match = re.search(pattern, message)
        if match:
            has_deadline = True
            deadline_text = match.group(0)
            if max_score < 7:
                max_score = 7
                level = "high"
            break
    
    # Exclamation marks increase urgency
    exclamation_count = message.count("!")
    if exclamation_count >= 3:
        max_score = min(10, max_score + 1)
        found_signals.append(f"علامات تعجب ({exclamation_count})")
    
    # ALL CAPS increases urgency
    upper_ratio = sum(1 for c in message if c.isupper()) / max(len(message), 1)
    if upper_ratio > 0.5:
        max_score = min(10, max_score + 1)
        found_signals.append("أحرف كبيرة (صراخ)")
    
    return level, max_score, found_signals, has_deadline, deadline_text


# ============ Sentiment Analysis ============

SENTIMENT_PATTERNS = {
    "positive": {
        "strong": ["ممتاز", "رائع", "مذهل", "أفضل", "سعيد جداً", "❤️", "👏", "🎉"],
        "mild": ["شكراً", "جيد", "حلو", "تمام", "مسرور", "راضي", "👍", "😊"],
    },
    "negative": {
        "strong": ["سيء جداً", "أسوأ", "كارثة", "محبط", "غاضب", "مستاء جداً", "😡", "💢"],
        "mild": ["للأسف", "مشكلة", "صعب", "متأخر", "غير راضي", "😔", "😞"],
    },
}

FRUSTRATION_SIGNALS = [
    "كم مرة", "مرة ثانية", "مرة أخرى", "لم أحصل", "لا جواب", "لا رد",
    "انتظرت", "أنتظر منذ", "من زمان", "حتى الآن", "لحد الآن",
]


def detect_sentiment(message: str) -> Tuple[str, float, List[str], int]:
    """
    Detect sentiment with score and emotional cues.
    Returns: (sentiment, score, cues, frustration_level)
    """
    positive_score = 0
    negative_score = 0
    cues = []
    
    for pattern in SENTIMENT_PATTERNS["positive"]["strong"]:
        if pattern in message:
            positive_score += 2
            cues.append(f"إيجابي قوي: {pattern}")
    
    for pattern in SENTIMENT_PATTERNS["positive"]["mild"]:
        if pattern in message:
            positive_score += 1
            cues.append(f"إيجابي: {pattern}")
    
    for pattern in SENTIMENT_PATTERNS["negative"]["strong"]:
        if pattern in message:
            negative_score += 2
            cues.append(f"سلبي قوي: {pattern}")
    
    for pattern in SENTIMENT_PATTERNS["negative"]["mild"]:
        if pattern in message:
            negative_score += 1
            cues.append(f"سلبي: {pattern}")
    
    # Calculate frustration
    frustration = 0
    for signal in FRUSTRATION_SIGNALS:
        if signal in message:
            frustration += 2
            cues.append(f"إحباط: {signal}")
    
    frustration = min(10, frustration)
    
    # Calculate final score (-1.0 to 1.0)
    total = positive_score + negative_score
    if total == 0:
        score = 0.0
        sentiment = "محايد"
    else:
        score = (positive_score - negative_score) / max(total, 1)
        score = max(-1.0, min(1.0, score))
        
        if score > 0.3:
            sentiment = "إيجابي"
        elif score < -0.3:
            sentiment = "سلبي"
        else:
            sentiment = "محايد"
    
    # Frustration affects sentiment
    if frustration >= 5 and sentiment != "سلبي":
        sentiment = "سلبي"
        score = min(score, -0.3)
    
    return sentiment, round(score, 2), cues, frustration


# ============ Entity Extraction ============

ENTITY_PATTERNS = {
    "phone_syria": r'(?:00963|\+963|0)?9\d{8}',
    "phone_saudi": r'(?:00966|\+966|0)?5\d{8}',
    "phone_uae": r'(?:00971|\+971|0)?5\d{8}',
    "phone_general": r'\+?\d{10,15}',
    "email": r'[\w\.-]+@[\w\.-]+\.\w+',
    "date": r'\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}',
    "time": r'\d{1,2}:\d{2}(?:\s*[صم])?',
    "money_syp": r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:ل\.س|ليرة سورية|ليرة)',
    "money_sar": r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:ر\.س|ريال|ريال سعودي)',
    "money_usd": r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:دولار|\$|USD)',
    "money_aed": r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:درهم|AED)',
    "order_number": r'(?:طلب|رقم الطلب|order)[\s#:]*([A-Z0-9\-]{5,20})',
    "invoice_number": r'(?:فاتورة|رقم الفاتورة|invoice)[\s#:]*([A-Z0-9\-]{5,20})',
    "url": r'https?://[^\s<>"{}|\\^`\[\]]+',
    "percentage": r'(\d+(?:\.\d+)?)\s*%',
    "quantity": r'(\d+)\s*(?:قطعة|حبة|كيلو|طن|متر|علبة|كرتون)',
}

NAME_PATTERNS = [
    r'(?:السيد|السيدة|الأستاذ|الأستاذة|المهندس|الدكتور)\s+([\u0600-\u06FF\s]{3,30})',
    r'(?:أنا|اسمي|أخوك|أخوكم)\s+([\u0600-\u06FF]{3,20})',
]

LOCATION_PATTERNS = [
    r'(?:العنوان|الموقع|في|إلى)[\s:]+([^،,\n]{5,50})',
    r'(?:شارع|حي|منطقة|مدينة)\s+([\u0600-\u06FF\s]{3,30})',
]


def extract_entities(message: str) -> Dict[str, Any]:
    """Extract all entities from message"""
    entities = {}
    
    # Extract using patterns
    for entity_type, pattern in ENTITY_PATTERNS.items():
        matches = re.findall(pattern, message, re.IGNORECASE)
        if matches:
            # Clean up matches
            if isinstance(matches[0], tuple):
                matches = [m[0] if isinstance(m, tuple) else m for m in matches]
            entities[entity_type] = list(set(matches))
    
    # Consolidate phone numbers
    phones = []
    for key in list(entities.keys()):
        if key.startswith("phone_"):
            phones.extend(entities.pop(key, []))
    if phones:
        entities["phones"] = list(set(phones))
    
    # Consolidate money
    money = []
    for key in list(entities.keys()):
        if key.startswith("money_"):
            currency = key.split("_")[1].upper()
            for amount in entities.pop(key, []):
                money.append({"amount": amount, "currency": currency})
    if money:
        entities["money"] = money
    
    # Extract names
    for pattern in NAME_PATTERNS:
        match = re.search(pattern, message)
        if match:
            entities["person_name"] = match.group(1).strip()
            break
    
    # Extract locations
    for pattern in LOCATION_PATTERNS:
        match = re.search(pattern, message)
        if match:
            location = match.group(1).strip()
            if len(location) > 5:
                entities["location"] = location
                break
    
    return entities


# ============ Question Detection ============

def extract_questions(message: str) -> List[str]:
    """Extract questions from the message"""
    questions = []
    
    # Split by question marks
    parts = re.split(r'[؟?]', message)
    for part in parts[:-1]:  # Last part won't be a question
        # Get the question part (from last period/newline)
        question = re.split(r'[.\n]', part)[-1].strip()
        if len(question) > 5:
            questions.append(question + "؟")
    
    # Look for question words without question mark
    question_words = ["كيف", "متى", "أين", "لماذا", "ما هي", "ما هو", "هل", "كم"]
    sentences = re.split(r'[.\n]', message)
    for sentence in sentences:
        sentence = sentence.strip()
        if any(sentence.startswith(qw) for qw in question_words):
            if sentence not in questions and len(sentence) > 5:
                questions.append(sentence)
    
    return questions[:5]  # Max 5 questions


# ============ Main Analysis Function ============

def analyze_message_advanced(message: str) -> AnalysisResult:
    """
    Perform comprehensive message analysis.
    Returns a detailed AnalysisResult dataclass.
    """
    # Intent detection
    primary_intent, secondary_intent, intent_confidence, intent_signals = detect_intent(message)
    
    # Urgency detection
    urgency_level, urgency_score, urgency_signals, has_deadline, deadline_text = detect_urgency(message)
    
    # Sentiment analysis
    sentiment, sentiment_score, emotional_cues, frustration_level = detect_sentiment(message)
    
    # Entity extraction
    entities = extract_entities(message)
    
    # Question extraction
    questions = extract_questions(message)
    
    # Language detection (simple)
    arabic_ratio = len(re.findall(r'[\u0600-\u06FF]', message)) / max(len(message), 1)
    language = "ar" if arabic_ratio > 0.3 else "en"
    
    # Dialect detection
    dialect = "فصحى"
    dialect_markers = {
        "شامي": ["شو", "كيفك", "هلق", "ليك", "منيح"],
        "خليجي": ["وش", "كذا", "زين", "واجد"],
        "مصري": ["إزيك", "كدة", "خالص", "قوي"],
    }
    for d, markers in dialect_markers.items():
        if any(m in message for m in markers):
            dialect = d
            break
    
    # Formality detection
    formal_markers = ["السيد", "المحترم", "نود", "يسرنا"]
    informal_markers = ["هاي", "هلا", "كيفك", "شو أخبارك"]
    
    formal_count = sum(1 for m in formal_markers if m in message)
    informal_count = sum(1 for m in informal_markers if m in message)
    
    if formal_count > informal_count:
        formality = "رسمي"
    elif informal_count > formal_count:
        formality = "غير رسمي"
    else:
        formality = "شبه رسمي"
    
    # Key points (first 3 sentences or bullet points)
    key_points = []
    bullets = re.findall(r'[-•*]\s*([^\n]+)', message)
    if bullets:
        key_points = bullets[:3]
    else:
        sentences = re.split(r'[.\n]', message)
        key_points = [s.strip() for s in sentences if len(s.strip()) > 10][:3]
    
    # Action items based on intent
    action_items = []
    if primary_intent == "استفسار":
        action_items = ["الرد على الاستفسار"]
    elif primary_intent == "طلب":
        action_items = ["معالجة الطلب", "تأكيد التفاصيل"]
    elif primary_intent == "شكوى":
        action_items = ["تسجيل الشكوى", "التواصل مع العميل", "حل المشكلة"]
    elif primary_intent == "متابعة":
        action_items = ["التحقق من الحالة", "إرسال تحديث"]
    
    # Metadata
    word_count = len(message.split())
    has_attachments = any(w in message.lower() for w in ["مرفق", "ملف", "صورة", "attachment", "attached"])
    is_reply = message.strip().startswith(("Re:", "رد:", ">>", ">"))
    is_forwarded = any(w in message.lower() for w in ["forwarded", "تحويل", "Fwd:"])
    
    return AnalysisResult(
        primary_intent=primary_intent,
        secondary_intent=secondary_intent,
        intent_confidence=intent_confidence,
        intent_signals=intent_signals,
        urgency_level=urgency_level,
        urgency_score=urgency_score,
        urgency_signals=urgency_signals,
        has_deadline=has_deadline,
        deadline_text=deadline_text,
        sentiment=sentiment,
        sentiment_score=sentiment_score,
        emotional_cues=emotional_cues,
        frustration_level=frustration_level,
        language=language,
        dialect=dialect,
        formality_level=formality,
        entities=entities,
        key_points=key_points,
        action_items=action_items,
        questions_asked=questions,
        word_count=word_count,
        has_attachments_mentioned=has_attachments,
        is_reply=is_reply,
        is_forwarded=is_forwarded,
    )


def analysis_to_dict(result: AnalysisResult) -> Dict[str, Any]:
    """Convert AnalysisResult to dictionary for JSON serialization"""
    return {
        "intent": {
            "primary": result.primary_intent,
            "secondary": result.secondary_intent,
            "confidence": result.intent_confidence,
            "signals": result.intent_signals,
        },
        "urgency": {
            "level": result.urgency_level,
            "score": result.urgency_score,
            "signals": result.urgency_signals,
            "has_deadline": result.has_deadline,
            "deadline": result.deadline_text,
        },
        "sentiment": {
            "label": result.sentiment,
            "score": result.sentiment_score,
            "cues": result.emotional_cues,
            "frustration_level": result.frustration_level,
        },
        "language": {
            "code": result.language,
            "dialect": result.dialect,
            "formality": result.formality_level,
        },
        "entities": result.entities,
        "summary": {
            "key_points": result.key_points,
            "action_items": result.action_items,
            "questions": result.questions_asked,
        },
        "metadata": {
            "word_count": result.word_count,
            "has_attachments": result.has_attachments_mentioned,
            "is_reply": result.is_reply,
            "is_forwarded": result.is_forwarded,
        },
    }
