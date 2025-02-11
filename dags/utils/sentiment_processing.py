from datetime import datetime, timezone
from typing import Dict, Any, List
from urllib.parse import urlparse
SENTIMENT_ASPECTS = ['price', 'adoption', 'regulation', 'technology']


def process_article(article: Dict[str, Any], analysis: Dict[str, Any]) -> Dict[str, Any]:
    aspects = analysis.get('sentiment_aspects', {})
    
    base_result = {
        'article_url': article['article_url'],
        'overall_sentiment': float(analysis.get('overall_sentiment')),
        'confidence_score': float(analysis.get('confidence_score')),
        'timestamp': analysis.get('timestamp', datetime.now(timezone.utc)),
        'model_version': analysis.get('model_version'),
        'sentiment_aspects': {
            aspect: {
                'sentiment': float(aspects.get(aspect, {}).get('sentiment', 0)) if aspects.get(aspect, {}).get('relevant') else None,
                'relevant': aspects.get(aspect, {}).get('relevant', False)
            }
            for aspect in SENTIMENT_ASPECTS
        },
        'raw_analysis': analysis.get('raw_analysis')
    }
    
    # Remove aspect-specific fields and clamping since they're now in sentiment_aspects
    for key in ['overall_sentiment', 'confidence_score']:
        if base_result[key] is not None:
            base_result[key] = max(-1.0, min(1.0, base_result[key]))
            
    return base_result


def get_failed_result(article_url: str) -> Dict[str, Any]:
    return {
        'article_url': article_url,
        'overall_sentiment': None,
        'confidence_score': None,
        'sentiment_aspects': {
            aspect: {'sentiment': None, 'relevant': False}
            for aspect in SENTIMENT_ASPECTS
        },
        'timestamp': datetime.now(timezone.utc),
        'model_version': 'failed',
        'raw_analysis': None
    }


def extract_source_from_url(url: str) -> str:
    """Extract domain from URL."""
    try:
        return urlparse(url).netloc.replace('www.', '')
    except:
        return 'unknown'