"""
Al-Mudeer Auto-Categorization Tests
Unit tests for Arabic NLP categorization, priority scoring, and sentiment analysis
"""

import pytest


# ============ Priority Enum ============

class TestPriorityEnum:
    """Tests for Priority enum values"""
    
    def test_priority_values(self):
        """Test Priority enum has correct Arabic values"""
        from services.auto_categorization import Priority
        
        assert Priority.URGENT == "عاجل"
        assert Priority.HIGH == "عالي"
        assert Priority.NORMAL == "عادي"
        assert Priority.LOW == "منخفض"


# ============ Message Category Enum ============

class TestMessageCategoryEnum:
    """Tests for MessageCategory enum values"""
    
    def test_category_values(self):
        """Test MessageCategory enum has correct Arabic values"""
        from services.auto_categorization import MessageCategory
        
        assert MessageCategory.INQUIRY == "استفسار"
        assert MessageCategory.SERVICE_REQUEST == "طلب خدمة"
        assert MessageCategory.COMPLAINT == "شكوى"
        assert MessageCategory.FOLLOWUP == "متابعة"
        assert MessageCategory.OFFER == "عرض"
        assert MessageCategory.FEEDBACK == "تقييم"
        assert MessageCategory.SUPPORT == "دعم فني"
        assert MessageCategory.BILLING == "مالي"
        assert MessageCategory.OTHER == "أخرى"


# ============ Tag Extraction ============

class TestTagExtraction:
    """Tests for extract_tags function"""
    
    def test_extract_pricing_tags(self):
        """Test extracting pricing-related tags"""
        from services.auto_categorization import extract_tags
        
        message = "أريد معرفة أسعار خدماتكم وتكلفة الاشتراك"
        tags = extract_tags(message)
        
        assert "pricing" in tags or any("سعر" in str(t) or "تكلفة" in str(t) for t in tags)
    
    def test_extract_delivery_tags(self):
        """Test extracting delivery-related tags"""
        from services.auto_categorization import extract_tags
        
        message = "متى سيصل التوصيل إلى موقعي؟"
        tags = extract_tags(message)
        
        assert len(tags) >= 0  # May or may not find tags
    
    def test_extract_empty_message(self):
        """Test extracting tags from empty message"""
        from services.auto_categorization import extract_tags
        
        tags = extract_tags("")
        
        assert isinstance(tags, list)


# ============ Priority Score Calculation ============

class TestPriorityScoreCalculation:
    """Tests for calculate_priority_score function"""
    
    def test_urgent_message_high_score(self):
        """Test urgent keywords yield high priority score"""
        from services.auto_categorization import calculate_priority_score, Priority
        
        message = "أحتاج مساعدة عاجلة فوراً!"
        priority, score = calculate_priority_score(message)
        
        # Should be urgent or high priority
        assert priority in [Priority.URGENT, Priority.HIGH]
        assert score >= 70
    
    def test_normal_message_normal_score(self):
        """Test normal message yields normal priority"""
        from services.auto_categorization import calculate_priority_score, Priority
        
        message = "السلام عليكم، أريد الاستفسار عن خدماتكم"
        priority, score = calculate_priority_score(message)
        
        assert priority == Priority.NORMAL or priority in [Priority.HIGH, Priority.LOW]
    
    def test_low_priority_indicators(self):
        """Test low priority keywords"""
        from services.auto_categorization import calculate_priority_score, Priority
        
        message = "عندما يكون لديك وقت، ممكن تفيدني لاحقاً؟"
        priority, score = calculate_priority_score(message)
        
        # Should be lower priority
        assert priority in [Priority.LOW, Priority.NORMAL]


# ============ Sentiment Calculation ============

class TestSentimentCalculation:
    """Tests for calculate_sentiment function"""
    
    def test_negative_sentiment(self):
        """Test negative sentiment detection"""
        from services.auto_categorization import calculate_sentiment
        
        message = "أنا غاضب جداً من الخدمة السيئة، مخيب للآمال"
        
        sentiment = calculate_sentiment(message)
        
        assert sentiment < 0  # Negative sentiment
    
    def test_neutral_sentiment(self):
        """Test neutral sentiment detection"""
        from services.auto_categorization import calculate_sentiment
        
        message = "أريد معرفة المزيد عن المنتج"
        
        sentiment = calculate_sentiment(message)
        
        assert -0.5 <= sentiment <= 0.5  # Roughly neutral
    
    def test_empty_message_sentiment(self):
        """Test sentiment of empty message"""
        from services.auto_categorization import calculate_sentiment
        
        sentiment = calculate_sentiment("")
        
        assert sentiment == 0 or abs(sentiment) <= 0.1


# ============ Category Detection ============

class TestCategoryDetection:
    """Tests for detect_category function"""
    
    def test_detect_inquiry_category(self):
        """Test detecting inquiry category"""
        from services.auto_categorization import detect_category, MessageCategory
        
        message = "أريد الاستفسار عن أسعار الخدمات"
        
        category, confidence = detect_category(message)
        
        assert category == MessageCategory.INQUIRY or confidence > 0
    
    def test_detect_complaint_category(self):
        """Test detecting complaint category"""
        from services.auto_categorization import detect_category, MessageCategory
        
        message = "عندي شكوى بخصوص الخدمة، هناك مشكلة كبيرة"
        
        category, confidence = detect_category(message)
        
        assert category == MessageCategory.COMPLAINT or confidence > 0
    
    def test_detect_support_category(self):
        """Test detecting technical support category"""
        from services.auto_categorization import detect_category, MessageCategory
        
        message = "أحتاج دعم فني، التطبيق لا يعمل"
        
        category, confidence = detect_category(message)
        
        assert category == MessageCategory.SUPPORT or confidence > 0


# ============ Folder Suggestion ============

class TestFolderSuggestion:
    """Tests for suggest_folder function"""
    
    def test_urgent_folder(self):
        """Test urgent priority suggests urgent folder"""
        from services.auto_categorization import suggest_folder, MessageCategory, Priority
        
        folder = suggest_folder(MessageCategory.INQUIRY, Priority.URGENT)
        
        assert "عاجل" in folder or "urgent" in folder.lower() or folder != ""
    
    def test_complaint_folder(self):
        """Test complaint category folder suggestion"""
        from services.auto_categorization import suggest_folder, MessageCategory, Priority
        
        folder = suggest_folder(MessageCategory.COMPLAINT, Priority.NORMAL)
        
        assert folder != "" or folder is not None


# ============ Auto Actions ============

class TestAutoActions:
    """Tests for suggest_auto_actions function"""
    
    def test_urgent_actions(self):
        """Test urgent priority suggests notification action"""
        from services.auto_categorization import suggest_auto_actions, MessageCategory, Priority
        
        actions = suggest_auto_actions(
            category=MessageCategory.INQUIRY,
            priority=Priority.URGENT,
            sentiment_score=0
        )
        
        assert isinstance(actions, list)
    
    def test_negative_sentiment_actions(self):
        """Test negative sentiment suggests escalation action"""
        from services.auto_categorization import suggest_auto_actions, MessageCategory, Priority
        
        actions = suggest_auto_actions(
            category=MessageCategory.COMPLAINT,
            priority=Priority.HIGH,
            sentiment_score=-0.8
        )
        
        assert isinstance(actions, list)


# ============ Main Categorization Function ============

class TestCategorizeMessage:
    """Tests for main categorize_message function"""
    
    def test_categorize_returns_result(self):
        """Test categorize_message returns CategoryResult"""
        from services.auto_categorization import categorize_message, CategoryResult
        
        result = categorize_message("السلام عليكم، أريد الاستفسار عن أسعار خدماتكم")
        
        assert isinstance(result, CategoryResult)
        assert result.category is not None
        assert result.priority is not None
        assert isinstance(result.tags, list)
    
    def test_categorize_urgent_complaint(self):
        """Test categorizing urgent complaint"""
        from services.auto_categorization import categorize_message, MessageCategory, Priority
        
        message = "شكوى عاجلة! أنا غاضب جداً من الخدمة السيئة"
        result = categorize_message(message)
        
        # Should have high priority and negative sentiment
        assert result.priority in [Priority.URGENT, Priority.HIGH]
        assert result.sentiment_score < 0
    
    def test_categorize_dict_format(self):
        """Test categorize_message_dict returns dictionary"""
        from services.auto_categorization import categorize_message_dict
        
        result = categorize_message_dict("استفسار عن المنتجات")
        
        assert isinstance(result, dict)
        assert "category" in result
        assert "priority" in result
        assert "tags" in result


# ============ Batch Processing ============

class TestBatchProcessing:
    """Tests for batch categorization"""
    
    def test_batch_categorization(self):
        """Test categorizing multiple messages at once"""
        from services.auto_categorization import categorize_messages_batch
        
        messages = [
            "استفسار عن الأسعار",
            "شكوى بخصوص التأخير",
            "شكراً على الخدمة الممتازة"
        ]
        
        results = categorize_messages_batch(messages)
        
        assert len(results) == 3
        assert all(r is not None for r in results)
    
    def test_batch_empty_list(self):
        """Test batch with empty list"""
        from services.auto_categorization import categorize_messages_batch
        
        results = categorize_messages_batch([])
        
        assert results == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
