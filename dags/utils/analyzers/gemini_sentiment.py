from typing import Dict, List
import google.generativeai as genai
from datetime import datetime, timezone
import logging
import json

class GeminiSentimentAnalyzer:
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash')
        
    def analyze_article(self, article_text: str, article_id: str, prompt: str = None, tries: int = 0) -> Dict:
        if tries >= 2:
            raise Exception(f"Failed to get valid response after {tries} attempts")

        logging.info(f"Analyzing article: {article_id[:50]}... (attempt {tries + 1}/2)")

        prompt = prompt or f"""You are a financial sentiment analyzer specializing in cryptocurrency news. Analyze this Bitcoin-related article and output ONLY a JSON object with these exact fields:

        {{
            "overall_sentiment": <float -1.0 to 1.0>,
            "confidence_score": <float 0.0 to 1.0>,
            "aspects": {{
                "price": {{
                    "sentiment": <float -1.0 to 1.0>,
                    "relevant": <boolean>
                }},
                "adoption": {{
                    "sentiment": <float -1.0 to 1.0>,
                    "relevant": <boolean>
                }},
                "regulation": {{
                    "sentiment": <float -1.0 to 1.0>,
                    "relevant": <boolean>
                }},
                "technology": {{
                    "sentiment": <float -1.0 to 1.0>,
                    "relevant": <boolean>
                }}
            }}
        }}

        Guidelines:
        - Sentiment: -1.0 (extremely negative) to 1.0 (extremely positive)
        - Mark aspects as "relevant": false if not mentioned
        - Focus on factual market impact, not speculation
        - Consider market context and historical patterns
        
        Article: {article_text}"""
        
        try:
            response = self.model.generate_content(prompt)
            
            cleaned_text = response.text.strip()
            # Remove markdown code block wrapper if present
            if cleaned_text.startswith('```'):
                # Remove first line containing ```json or similar
                cleaned_text = cleaned_text.split('\n', 1)[1]
                # Remove last line containing ```
                cleaned_text = cleaned_text.rsplit('\n', 1)[0]
            
            # Clean up JSON formatting issues
            cleaned_text = (cleaned_text
                .replace('\n', '')           # Remove newlines
                .replace(' ', '')            # Remove spaces
                .replace('\t', '')           # Remove tabs
                .replace('}{', '},{')        # Fix missing commas
                .replace('""', '"')          # Fix double quotes
                .replace(',,', ',')          # Fix double commas
            )
            
            analysis = json.loads(cleaned_text)
            
            return {
                "article_id": article_id,
                "timestamp": datetime.now(timezone.utc),
                "overall_sentiment": analysis.get("overall_sentiment") or analysis.get("sentiment"),
                "confidence_score": analysis.get("confidence_score") or analysis.get("confidence"),
                "sentiment_aspects": analysis.get("aspects", {}),
                "raw_analysis": response.text,
                "model_version": "gemini-2.0-flash-lite"
            }
        except (json.JSONDecodeError, Exception) as e:
            logging.error(f"Error ({type(e).__name__}): {str(e)}")
            if isinstance(e, json.JSONDecodeError):
                logging.error(f"Failed text: {repr(cleaned_text)}")
            
            retry_prompt = f"""Your previous response was invalid. Return ONLY a JSON object. No explanation or markdown.
{{
    "overall_sentiment": <float -1.0 to 1.0>,
    "confidence_score": <float 0.0 to 1.0>,
    "aspects": {{
        "price": {{"sentiment": <float>, "relevant": <boolean>}},
        "adoption": {{"sentiment": <float>, "relevant": <boolean>}},
        "regulation": {{"sentiment": <float>, "relevant": <boolean>}},
        "technology": {{"sentiment": <float>, "relevant": <boolean>}}
    }}
}}

Article: {article_text}"""

            return self.analyze_article(article_text, article_id, retry_prompt, tries + 1)