from typing import Dict, List
import google.generativeai as genai
from datetime import datetime, timezone
import logging
import json

class GeminiSentimentAnalyzer:
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash-lite-preview-02-05')
        
    def analyze_article(self, article_text: str, article_id: str) -> Dict:
        logging.info(f"Analyzing article: {article_id[:50]}...")
        
        prompt = f"""You are a financial sentiment analyzer specializing in cryptocurrency news. Analyze this Bitcoin-related article and output ONLY a JSON object with these exact fields:

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
                "overall_sentiment": analysis["overall_sentiment"],
                "confidence_score": analysis["confidence_score"],
                "sentiment_aspects": analysis["aspects"],
                "raw_analysis": response.text,
                "model_version": "gemini-2.0-flash-lite"
            }
        except json.JSONDecodeError as e:
            logging.error(f"JSON Parse Error: {str(e)}")
            logging.error(f"Failed text: {repr(cleaned_text)}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            logging.error(f"Error type: {type(e)}")
            raise 