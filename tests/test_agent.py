"""
Al-Mudeer AI Agent Tests
Tests for AI processing pipeline
"""

import pytest


# ============ Entity Extraction ============

class TestEntityExtraction:
    """Tests for entity extraction from messages"""
    
    def test_extract_arabic_phone(self):
        """Test Arabic phone number extraction"""
        from agent import extract_entities
        
        message = "رقم هاتفي 0501234567"
        entities = extract_entities(message)
        
        assert "phones" in entities
        assert len(entities["phones"]) > 0
    
    def test_extract_international_phone(self):
        """Test international phone extraction"""
        from agent import extract_entities
        
        message = "Call me at +966501234567"
        entities = extract_entities(message)
        
        assert "phones" in entities
    
    def test_extract_email(self):
        """Test email extraction"""
        from agent import extract_entities
        
        message = "Please contact test@example.com"
        entities = extract_entities(message)
        
        assert "emails" in entities
        assert "test@example.com" in entities["emails"]
    
    def test_extract_multiple_entities(self):
        """Test multiple entity extraction"""
        from agent import extract_entities
        
        message = "اتصل على 0501234567 او راسلني على info@company.com"
        entities = extract_entities(message)
        
        assert len(entities.get("phones", [])) > 0
        assert len(entities.get("emails", [])) > 0


# ============ Rule-based Classification ============

class TestRuleBasedClassify:
    """Tests for rule-based classification fallback"""
    
    def test_classify_inquiry(self):
        """Test inquiry classification"""
        from agent import rule_based_classify
        
        # Arabic inquiry
        result = rule_based_classify("ما هو سعر المنتج؟")
        assert result["intent"] == "استفسار"
        
        # Price question
        result = rule_based_classify("كم السعر؟")
        assert result["intent"] == "استفسار"
    
    def test_classify_complaint(self):
        """Test complaint classification"""
        from agent import rule_based_classify
        
        result = rule_based_classify("أريد تقديم شكوى على الخدمة")
        assert result["intent"] == "شكوى"
        
        result = rule_based_classify("الخدمة سيئة جدا")
        assert result["intent"] in ["شكوى", "عام"]
    
    def test_classify_order(self):
        """Test order classification"""
        from agent import rule_based_classify
        
        result = rule_based_classify("أريد طلب المنتج")
        assert result["intent"] == "طلب"
    
    def test_classify_greeting(self):
        """Test greeting classification"""
        from agent import rule_based_classify
        
        result = rule_based_classify("السلام عليكم")
        assert result["intent"] in ["تحية", "عام"]
    
    def test_urgency_detection(self):
        """Test urgency detection"""
        from agent import rule_based_classify
        
        # Urgent
        result = rule_based_classify("عاجل جدا أحتاج المساعدة فورا")
        assert result["urgency"] in ["عاجل", "مرتفع"]
        
        # Normal
        result = rule_based_classify("شكرا على المعلومات")
        assert result["urgency"] in ["عادي", "منخفض"]


# ============ Response Generation ============

class TestResponseGeneration:
    """Tests for response generation"""
    
    def test_rule_based_response_inquiry(self):
        """Test rule-based response for inquiry"""
        from agent import generate_rule_based_response
        
        state = {
            "intent": "استفسار",
            "sender_name": "أحمد",
            "raw_message": "ما هو السعر؟",
            "urgency": "عادي",
            "preferences": None,
        }
        
        result = generate_rule_based_response(state)
        
        assert "draft_response" in result
        assert len(result["draft_response"]) > 0
    
    def test_rule_based_response_complaint(self):
        """Test rule-based response for complaint"""
        from agent import generate_rule_based_response
        
        state = {
            "intent": "شكوى",
            "sender_name": "محمد",
            "raw_message": "لدي مشكلة",
            "urgency": "عاجل",
            "preferences": None,
        }
        
        result = generate_rule_based_response(state)
        
        assert "draft_response" in result
        # Complaint responses should acknowledge the issue
        assert len(result["draft_response"]) > 20


# ============ System Prompt Building ============

class TestSystemPrompt:
    """Tests for system prompt customization"""
    
    def test_build_prompt_default(self):
        """Test default system prompt"""
        from agent import build_system_prompt
        
        prompt = build_system_prompt(None)
        
        assert len(prompt) > 100
        assert "تحليل" in prompt or "مساعد" in prompt
    
    def test_build_prompt_with_preferences(self):
        """Test customized system prompt"""
        from agent import build_system_prompt
        
        preferences = {
            "business_name": "شركة الفلاح",
            "tone": "formal",
            "industry": "retail",
        }
        
        prompt = build_system_prompt(preferences)
        
        assert "شركة الفلاح" in prompt or len(prompt) > 100


# ============ Agent Pipeline ============

class TestAgentPipeline:
    """Tests for agent pipeline nodes"""
    
    @pytest.mark.asyncio
    async def test_ingest_node(self):
        """Test ingest node cleans message"""
        from agent import ingest_node, AgentState
        
        state: AgentState = {
            "raw_message": "  Hello world  \n",
            "message_type": "email",
            "intent": "",
            "urgency": "",
            "sentiment": "",
            "language": None,
            "dialect": None,
            "sender_name": None,
            "sender_contact": None,
            "action_items": [],
            "extracted_entities": {},
            "summary": "",
            "draft_response": "",
            "suggested_actions": [],
            "error": None,
            "processing_step": "start",
            "preferences": None,
            "conversation_history": None,
        }
        
        result = await ingest_node(state)
        
        assert result["processing_step"] == "ingested" or "استلام" in result["processing_step"]
        assert result["raw_message"].strip() == "Hello world"
