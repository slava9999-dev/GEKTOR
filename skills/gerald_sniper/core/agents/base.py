from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod
from core.scenario.signal_entity import TradingSignal

class AgentVote(BaseModel):
    """
    Independent Agent Evaluation (Task 7.1).
    Scores from -1.0 (Strong Sell/Against) to 1.0 (Strong Buy/For).
    """
    agent_name: str
    score: float = 0.0 # -1.0 to 1.0
    confidence: float = 1.0 # 0.0 to 1.0
    is_veto: bool = False # Hard block if True
    reason: str = "Neutral"

class IAgent(ABC):
    """
    Abstract Base for Hive Mind Agents (Task 7.1).
    """
    def __init__(self, name: str, weight: float = 1.0):
        self.name = name
        self.weight = weight

    @abstractmethod
    async def evaluate(self, signal: TradingSignal) -> AgentVote:
        """
        Independent evaluation logic.
        """
        pass
