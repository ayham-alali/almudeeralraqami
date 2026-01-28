"""
Al-Mudeer Smart Task AI Service
Analyzes task titles to suggest priority, subtasks, and categorization.
"""

import os
import json
import logging
from typing import Dict, List, Optional
from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

# Initialize Gemini Client
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

TASK_ANALYSIS_PROMPT = """
You are an intelligent task assistant for a Muslim Arab user. 
Analyze the following task title and provide structured suggestions.

Task Title: "{task_title}"

Output JSON format:
{{
    "priority": "high" | "medium" | "low",
    "color": int (ARGB integer value),
    "subtasks": ["subtask 1", "subtask 2", "subtask 3"]
}}

Rules:
1. Priority:
   - "High" if urgency is detected (e.g., "urgent", "today", "asap", words like "عاجل", "اليوم").
   - "High" for religious obligations (e.g., "Prayer").
   - "Medium" default.
   
2. Color (ARGB Int):
   - Red (0xFFFF5252) for High Priority/Urgent.
   - Green (0xFF4CAF50) for Religious/Personal.
   - Blue (0xFF2196F3) for Work/General.
   - Orange (0xFFFF9800) for Medium/Warning.
   
3. Subtasks:
   - Break down the task into 3-5 actionable steps.
   - If the task is simple, return an empty list or 1 step.
   - Respond in the SAME LANGUAGE as the task title (Arabic or English).

Example:
Input: "Prepare for Ramadan"
Output: {{
    "priority": "high",
    "color": 4283215696,  // Green 0xFF4CAF50
    "subtasks": ["Create grocery list", "Clean the house", "Set up prayer area"]
}}
"""

async def analyze_task_intent(task_title: str) -> Dict:
    """
    Analyze a task title using Gemini Flash to suggest attributes.
    """
    try:
        # Construct prompt
        prompt = TASK_ANALYSIS_PROMPT.format(task_title=task_title)
        
        # Call Gemini (using Flash for speed/cost)
        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.3
            )
        )
        
        # Parse result
        if response.text:
            result = json.loads(response.text)
            return {
                "priority": result.get("priority", "medium"),
                "color": result.get("color"),
                "sub_tasks": result.get("subtasks", [])
            }
            
    except Exception as e:
        logger.error(f"Task AI analysis failed: {e}")
        # Fallback defaults
        return {
            "priority": "medium",
            "color": None,
            "sub_tasks": []
        }
    
    return {"priority": "medium", "color": None, "sub_tasks": []}
