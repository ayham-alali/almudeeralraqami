"""
Al-Mudeer Style Learning Module
Learn and adapt to user's writing style from their past messages
"""

import json
import re
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import os


@dataclass
class StyleProfile:
    """Learned writing style profile from user's messages"""
    
    # Basic info
    profile_id: str
    license_id: str
    created_at: str
    updated_at: str
    message_count: int  # Number of messages analyzed
    
    # Tone & formality
    formality_level: str  # formal, semi-formal, casual
    warmth_level: str  # warm, neutral, professional
    
    # Language patterns
    primary_language: str  # ar, en, etc.
    dialect: str  # شامي، خليجي، مصري، فصحى
    uses_emojis: bool
    emoji_frequency: str  # never, rare, occasional, frequent
    
    # Structure patterns
    avg_response_length: int  # characters
    preferred_length: str  # short, medium, long
    uses_bullet_points: bool
    uses_numbered_lists: bool
    
    # Signature patterns
    common_greetings: List[str]  # Top 3 greetings used
    common_closings: List[str]  # Top 3 closings used
    signature_line: Optional[str]  # If they have a consistent signature
    
    # Phrase patterns
    favorite_phrases: List[str]  # Common phrases they repeat
    transition_words: List[str]  # How they connect ideas
    acknowledgment_style: str  # How they acknowledge receipt
    
    # Personality traits detected
    personality_traits: List[str]  # e.g., "direct", "empathetic", "detailed"
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'StyleProfile':
        return cls(**data)
    
    def to_prompt(self) -> str:
        """Convert profile to prompt instructions for LLM"""
        prompt_parts = []
        
        # Formality
        formality_desc = {
            "formal": "استخدم لغة رسمية ومهنية",
            "semi-formal": "استخدم لغة شبه رسمية، ودودة لكن محترمة",
            "casual": "استخدم لغة عفوية وودودة"
        }
        prompt_parts.append(formality_desc.get(self.formality_level, ""))
        
        # Dialect
        if self.dialect and self.dialect != "فصحى":
            prompt_parts.append(f"استخدم اللهجة {self.dialect} الخفيفة عند الحاجة")
        
        # Length
        length_desc = {
            "short": "اجعل الرد قصيراً ومختصراً (2-3 أسطر)",
            "medium": "اجعل الرد متوسط الطول (4-6 أسطر)",
            "long": "يمكن أن يكون الرد مفصلاً عند الحاجة"
        }
        prompt_parts.append(length_desc.get(self.preferred_length, ""))
        
        # Greetings
        if self.common_greetings:
            prompt_parts.append(f"استخدم تحيات مثل: {', '.join(self.common_greetings[:2])}")
        
        # Closings
        if self.common_closings:
            prompt_parts.append(f"اختتم بعبارات مثل: {', '.join(self.common_closings[:2])}")
        
        # Favorite phrases
        if self.favorite_phrases:
            prompt_parts.append(f"يمكنك استخدام عبارات مثل: {', '.join(self.favorite_phrases[:3])}")
        
        # Emojis
        if self.uses_emojis and self.emoji_frequency in ["occasional", "frequent"]:
            prompt_parts.append("يمكنك إضافة إيموجي خفيف عند المناسبة 👍")
        elif not self.uses_emojis:
            prompt_parts.append("لا تستخدم إيموجي")
        
        # Personality
        if self.personality_traits:
            traits_desc = {
                "direct": "كن مباشراً في الردود",
                "empathetic": "أظهر تعاطفاً واهتماماً",
                "detailed": "قدم تفاصيل وشرحاً كافياً",
                "friendly": "كن ودوداً وقريباً",
                "formal": "حافظ على الطابع الرسمي",
            }
            for trait in self.personality_traits[:2]:
                if trait in traits_desc:
                    prompt_parts.append(traits_desc[trait])
        
        return "\n".join([p for p in prompt_parts if p])


# ============ Analysis Functions ============

async def analyze_messages_for_style(
    messages: List[Dict[str, Any]],
    license_id: str,
) -> StyleProfile:
    """
    Analyze a list of sent messages to extract style patterns.
    
    messages should be a list of dicts with at least:
    - body: str (the message content)
    - channel: str (email, telegram, whatsapp)
    - sent_at: str (timestamp)
    """
    if not messages:
        return create_default_profile(license_id)
    
    # Extract all message bodies
    bodies = [m.get("body", "") for m in messages if m.get("body")]
    
    if len(bodies) < 3:
        # Not enough data
        return create_default_profile(license_id)
    
    # Analyze patterns
    analysis = {
        "formality_level": analyze_formality(bodies),
        "warmth_level": analyze_warmth(bodies),
        "primary_language": detect_primary_language(bodies),
        "dialect": detect_dialect(bodies),
        "uses_emojis": any(has_emojis(b) for b in bodies),
        "emoji_frequency": analyze_emoji_frequency(bodies),
        "avg_response_length": sum(len(b) for b in bodies) // len(bodies),
        "preferred_length": categorize_length(sum(len(b) for b in bodies) // len(bodies)),
        "uses_bullet_points": any("•" in b or "-" in b for b in bodies),
        "uses_numbered_lists": any(re.search(r'\d+[.)]\s', b) for b in bodies),
        "common_greetings": extract_common_greetings(bodies),
        "common_closings": extract_common_closings(bodies),
        "signature_line": extract_signature(bodies),
        "favorite_phrases": extract_favorite_phrases(bodies),
        "transition_words": extract_transition_words(bodies),
        "acknowledgment_style": detect_acknowledgment_style(bodies),
        "personality_traits": detect_personality_traits(bodies),
    }
    
    return StyleProfile(
        profile_id=f"style_{license_id}_{datetime.now().strftime('%Y%m%d')}",
        license_id=license_id,
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat(),
        message_count=len(bodies),
        **analysis
    )


def create_default_profile(license_id: str) -> StyleProfile:
    """Create a default style profile when no data available"""
    return StyleProfile(
        profile_id=f"style_{license_id}_default",
        license_id=license_id,
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat(),
        message_count=0,
        formality_level="semi-formal",
        warmth_level="neutral",
        primary_language="ar",
        dialect="فصحى",
        uses_emojis=False,
        emoji_frequency="never",
        avg_response_length=200,
        preferred_length="medium",
        uses_bullet_points=False,
        uses_numbered_lists=False,
        common_greetings=["مرحباً", "أهلاً"],
        common_closings=["مع التحية", "شكراً"],
        signature_line=None,
        favorite_phrases=[],
        transition_words=["بخصوص", "بالنسبة لـ"],
        acknowledgment_style="formal",
        personality_traits=["professional"],
    )


# ============ Analysis Helpers ============

def analyze_formality(texts: List[str]) -> str:
    """Detect formality level from texts"""
    formal_markers = ["السيد", "السيدة", "المحترم", "نود إفادتكم", "يسرنا"]
    casual_markers = ["هلا", "كيفك", "شو", "وين", "ليش", "هاي"]
    
    formal_count = sum(1 for t in texts for m in formal_markers if m in t)
    casual_count = sum(1 for t in texts for m in casual_markers if m in t)
    
    if formal_count > casual_count * 2:
        return "formal"
    elif casual_count > formal_count:
        return "casual"
    return "semi-formal"


def analyze_warmth(texts: List[str]) -> str:
    """Detect warmth level"""
    warm_markers = ["حبيبي", "عزيزي", "يا طيب", "الله يعطيك العافية", "❤️", "😊"]
    professional_markers = ["تحياتي", "مع التقدير", "نقدر تعاونكم"]
    
    warm_count = sum(1 for t in texts for m in warm_markers if m in t)
    pro_count = sum(1 for t in texts for m in professional_markers if m in t)
    
    if warm_count > len(texts) * 0.3:
        return "warm"
    elif pro_count > warm_count:
        return "professional"
    return "neutral"


def detect_primary_language(texts: List[str]) -> str:
    """Detect primary language"""
    arabic_pattern = re.compile(r'[\u0600-\u06FF]')
    
    arabic_chars = sum(len(arabic_pattern.findall(t)) for t in texts)
    total_chars = sum(len(t) for t in texts)
    
    if total_chars == 0:
        return "ar"
    
    if arabic_chars / total_chars > 0.5:
        return "ar"
    return "en"


def detect_dialect(texts: List[str]) -> str:
    """Detect Arabic dialect"""
    dialect_markers = {
        "شامي": ["شو", "كيفك", "هلق", "منيح", "كتير", "ليك"],
        "خليجي": ["وش", "كذا", "زين", "واجد", "حبيبي"],
        "مصري": ["إزيك", "كدة", "خالص", "قوي", "يعني"],
    }
    
    scores = {d: 0 for d in dialect_markers}
    
    for text in texts:
        text_lower = text.lower()
        for dialect, markers in dialect_markers.items():
            for marker in markers:
                if marker in text_lower:
                    scores[dialect] += 1
    
    max_dialect = max(scores, key=scores.get)
    if scores[max_dialect] > len(texts) * 0.1:
        return max_dialect
    return "فصحى"


def has_emojis(text: str) -> bool:
    """Check if text contains emojis"""
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002702-\U000027B0"
        "]+",
        flags=re.UNICODE
    )
    return bool(emoji_pattern.search(text))


def analyze_emoji_frequency(texts: List[str]) -> str:
    """Analyze how often emojis are used"""
    emoji_count = sum(1 for t in texts if has_emojis(t))
    ratio = emoji_count / len(texts) if texts else 0
    
    if ratio == 0:
        return "never"
    elif ratio < 0.1:
        return "rare"
    elif ratio < 0.4:
        return "occasional"
    return "frequent"


def categorize_length(avg_length: int) -> str:
    """Categorize average response length"""
    if avg_length < 150:
        return "short"
    elif avg_length < 400:
        return "medium"
    return "long"


def extract_common_greetings(texts: List[str]) -> List[str]:
    """Extract common greeting patterns"""
    greeting_patterns = [
        r'^(مرحباً?|أهلاً?|السلام عليكم|هلا|صباح الخير|مساء الخير)',
        r'^(تحية طيبة|السيد|السيدة)',
    ]
    
    greetings = {}
    for text in texts:
        first_line = text.split('\n')[0][:50]
        for pattern in greeting_patterns:
            match = re.match(pattern, first_line)
            if match:
                greeting = match.group(1)
                greetings[greeting] = greetings.get(greeting, 0) + 1
    
    # Return top 3
    sorted_greetings = sorted(greetings.items(), key=lambda x: -x[1])
    return [g[0] for g in sorted_greetings[:3]]


def extract_common_closings(texts: List[str]) -> List[str]:
    """Extract common closing patterns"""
    closing_patterns = [
        r'(مع التحية|شكراً?|تحياتي|بالتوفيق|مع التقدير)[\s\n]*$',
        r'(موجودين لأي سؤال|تواصل معنا)[\s\n]*$',
    ]
    
    closings = {}
    for text in texts:
        last_lines = '\n'.join(text.split('\n')[-2:])
        for pattern in closing_patterns:
            match = re.search(pattern, last_lines)
            if match:
                closing = match.group(1)
                closings[closing] = closings.get(closing, 0) + 1
    
    sorted_closings = sorted(closings.items(), key=lambda x: -x[1])
    return [c[0] for c in sorted_closings[:3]]


def extract_signature(texts: List[str]) -> Optional[str]:
    """Extract consistent signature line if present"""
    # Look for lines that appear in many messages at the end
    endings = []
    for text in texts:
        lines = text.strip().split('\n')
        if len(lines) >= 2:
            endings.append(lines[-1].strip())
    
    if not endings:
        return None
    
    # Find most common ending
    from collections import Counter
    counter = Counter(endings)
    most_common = counter.most_common(1)[0]
    
    # If it appears in more than 50% of messages, it's likely a signature
    if most_common[1] > len(texts) * 0.5:
        return most_common[0]
    return None


def extract_favorite_phrases(texts: List[str]) -> List[str]:
    """Extract frequently used phrases"""
    # Common phrase patterns
    phrase_patterns = [
        r'(إن شاء الله)',
        r'(الله يعطيك العافية)',
        r'(ما في مشكلة)',
        r'(تمام)',
        r'(حاضر)',
        r'(بالضبط)',
        r'(طيب)',
        r'(أكيد)',
    ]
    
    phrases = {}
    for text in texts:
        for pattern in phrase_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                phrases[match] = phrases.get(match, 0) + 1
    
    sorted_phrases = sorted(phrases.items(), key=lambda x: -x[1])
    return [p[0] for p in sorted_phrases[:5]]


def extract_transition_words(texts: List[str]) -> List[str]:
    """Extract common transition words"""
    transitions = ["بخصوص", "بالنسبة لـ", "أما عن", "من ناحية", "بالإضافة"]
    
    found = {}
    for text in texts:
        for trans in transitions:
            if trans in text:
                found[trans] = found.get(trans, 0) + 1
    
    sorted_trans = sorted(found.items(), key=lambda x: -x[1])
    return [t[0] for t in sorted_trans[:3]]


def detect_acknowledgment_style(texts: List[str]) -> str:
    """Detect how they acknowledge receiving messages"""
    formal_ack = ["تم استلام", "وصلتنا رسالتكم", "نشكركم على تواصلكم"]
    casual_ack = ["وصلتني", "شفت رسالتك", "تمام"]
    
    formal_count = sum(1 for t in texts for a in formal_ack if a in t)
    casual_count = sum(1 for t in texts for a in casual_ack if a in t)
    
    if formal_count > casual_count:
        return "formal"
    elif casual_count > formal_count:
        return "casual"
    return "balanced"


def detect_personality_traits(texts: List[str]) -> List[str]:
    """Detect personality traits from writing style"""
    traits = []
    
    # Direct vs detailed
    avg_length = sum(len(t) for t in texts) / len(texts) if texts else 0
    if avg_length < 150:
        traits.append("direct")
    elif avg_length > 400:
        traits.append("detailed")
    
    # Empathetic markers
    empathy_markers = ["أفهم", "معك حق", "أقدر", "آسف"]
    if sum(1 for t in texts for m in empathy_markers if m in t) > len(texts) * 0.2:
        traits.append("empathetic")
    
    # Friendly markers
    friendly_markers = ["😊", "👍", "هلا", "حبيبي", "يا طيب"]
    if sum(1 for t in texts for m in friendly_markers if m in t) > len(texts) * 0.2:
        traits.append("friendly")
    
    if not traits:
        traits.append("professional")
    
    return traits[:3]


# ============ Storage Functions ============

async def save_style_profile(profile: StyleProfile, db) -> bool:
    """Save style profile to database"""
    from db_helper import execute_sql
    
    try:
        await execute_sql(db, """
            INSERT OR REPLACE INTO style_profiles 
            (profile_id, license_id, profile_data, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            profile.profile_id,
            profile.license_id,
            json.dumps(profile.to_dict(), ensure_ascii=False),
            profile.created_at,
            profile.updated_at,
        ))
        return True
    except Exception as e:
        print(f"Error saving style profile: {e}")
        return False


async def get_style_profile(license_id: str, db) -> Optional[StyleProfile]:
    """Get style profile from database"""
    from db_helper import execute_sql
    
    try:
        result = await execute_sql(db, """
            SELECT profile_data FROM style_profiles
            WHERE license_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
        """, (license_id,))
        
        if result and len(result) > 0:
            data = json.loads(result[0][0])
            return StyleProfile.from_dict(data)
    except Exception as e:
        print(f"Error getting style profile: {e}")
    
    return None


async def init_style_profiles_table(db):
    """Initialize the style_profiles table"""
    from db_helper import execute_sql
    
    await execute_sql(db, """
        CREATE TABLE IF NOT EXISTS style_profiles (
            profile_id TEXT PRIMARY KEY,
            license_id TEXT NOT NULL,
            profile_data TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT,
            UNIQUE(license_id)
        )
    """)
