import asyncio
import sys
import unittest
from unittest.mock import AsyncMock, patch, MagicMock
import json

# Adjust path to import backend modules
sys.path.append("C:/Projects/almudeer/backend")

from agent_enhanced import enhanced_verify_node, EnhancedAgentState

class TestActorCritic(unittest.IsolatedAsyncioTestCase):
    
    async def test_verification_catches_hallucination(self):
        """Test that verify_node catches a hallucinated price"""
        
        # State with a dummy response that contains a fake price
        state: EnhancedAgentState = {
            "raw_message": "كم سعر الاشتراك؟",
            "draft_response": "سعر الاشتراك هو 500 دولار شهرياً.",
            "extracted_entities": {"interest": "subscription"},
            "preferences": {"business_name": "Al-Mudeer"},
            "response_quality_score": 100,
            "response_quality_issues": [],
            "summary": "User asked about price",
            "error": None,
            "processing_step": "draft"
        }
        
        # Mock LLM to act as the Critic and reject the response
        mock_critic_response = json.dumps({
            "is_valid": False,
            "score": 40,
            "reason": "Hallucination: Price of 500 USD is not mentioned in facts.",
            "critic_feedback": "Remove the price and ask the user to contact sales."
        })
        
        with patch('agent_enhanced.call_llm_enhanced', new_callable=AsyncMock) as mock_llm:
            mock_llm.return_value = mock_critic_response
            
            # Run the node
            result = await enhanced_verify_node(state)
            
            # Assertions
            self.assertEqual(result["response_quality_score"], 40)
            self.assertIn("Hallucination", result["response_quality_issues"][0])
            self.assertIsNotNone(result["error"])
            self.assertIn("Verification failed", result["error"])
            print("Verified: Critic successfully caught the hallucination!")

    async def test_verification_passes_good_response(self):
        """Test that verify_node passes a correct response"""
        
        state: EnhancedAgentState = {
            "raw_message": "مرحبا",
            "draft_response": "أهلاً بك! كيف يمكنني مساعدتك اليوم؟ 😊",
            "extracted_entities": {},
            "preferences": {},
            "response_quality_score": 100,
            "response_quality_issues": [],
            "summary": "Greeting",
            "error": None,
            "processing_step": "draft"
        }
        
        mock_critic_response = json.dumps({
            "is_valid": True,
            "score": 95,
            "reason": "Professional and polite.",
            "critic_feedback": ""
        })
        
        with patch('agent_enhanced.call_llm_enhanced', new_callable=AsyncMock) as mock_llm:
            mock_llm.return_value = mock_critic_response
            
            # Run the node
            result = await enhanced_verify_node(state)
            
            # Assertions
            self.assertEqual(result["response_quality_score"], 95)
            self.assertEqual(len(result["response_quality_issues"]), 0)
            self.assertIsNone(result["error"])
            print("Verified: Critic successfully passed the high-quality response!")

if __name__ == "__main__":
    unittest.main()
