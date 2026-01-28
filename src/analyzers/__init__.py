"""
Data analyzers for Decision-Ready Restaurant Reports
"""

# Import analyzers only when needed to avoid circular imports
def get_ai_analyzer():
    from .ai_analyzer import AIAnalyzer
    return AIAnalyzer