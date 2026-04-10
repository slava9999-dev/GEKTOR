import asyncio
from typing import List, Dict, Tuple, Optional
from loguru import logger
from .base import IAgent, AgentVote
from core.scenario.signal_entity import TradingSignal

class DebateChamber:
    """
    Debate Chamber (Task 7.2 & STRESS TEST).
    Runs specialized agents in parallel with strict HFT-grade timeouts.
    """
    CONSENSUS_THRESHOLD = 0.65 # Minimal confidence/score product

    def __init__(self, agents: List[IAgent]):
        self.agents = agents
        # Strict timeout as per STRESS TEST (Rule 7.2)
        self.alpha_decay_timeout_sec = 0.05 # 50 milliseconds

    async def hold_debate(self, signal: TradingSignal) -> Tuple[bool, float, str]:
        """
        Executes parallel agent evaluations.
        Returns: (passed: bool, weighted_score: float, reason: str)
        """
        # 1. Spawn all agents concurrently (Task 7.2)
        tasks = [agent.evaluate(signal) for agent in self.agents]
        
        # 2. Strict HFT-grade wait (Stress Test Response)
        try:
            # We wait for ALL but with a deadline. (wait_for/gather + timeout)
            # Actually asyncio.gather doesn't support built-in per-task timeout well.
            # We'll use wait() with a timeout to catch slow LLM/API agents.
            done, pending = await asyncio.wait(
                tasks, 
                timeout=self.alpha_decay_timeout_sec,
                return_when=asyncio.ALL_COMPLETED
            )
            
            # Cancel slow agents to free GIL/resources (Stress Test)
            for p in pending:
                 p.cancel()
                 logger.warning(f"🕒 [Chamber] Sluggish agent timed out on {signal.symbol}: {p}")

            votes: List[AgentVote] = []
            for d in done:
                 try:
                     votes.append(d.result())
                 except Exception as e:
                     logger.error(f"❌ [Chamber] Agent crashed: {e}")

            if not votes:
                return False, 0.0, "No agent consensus reached within deadline"

            # 3. Consensus Logic (Weighted Sum)
            total_score = 0.0
            total_weight = 0.0
            
            for vote in votes:
                # VETO check (Fail Fast)
                if vote.is_veto:
                    logger.warning(f"🚫 [Chamber] VETO by {vote.agent_name}: {vote.reason}")
                    return False, -1.0, f"VETO: {vote.agent_name} - {vote.reason}"
                
                # Retrieve agent weight (we matched by name for simplicity or we'd store a map)
                agent_weight = next((a.weight for a in self.agents if a.name == vote.agent_name), 1.0)
                
                # Weighted contribution: score * confidence * weight
                # In HIVE MIND, we want sign(score) to be consistent with signal direction
                # but IAgent.evaluate already handles its own logic.
                contribution = vote.score * vote.confidence * agent_weight
                total_score += contribution
                total_weight += agent_weight

            final_score = total_score / total_weight if total_weight > 0 else 0.0
            
            if final_score >= self.CONSENSUS_THRESHOLD:
                logger.info(f"🧠 [Chamber] Consensus reached for {signal.symbol}: {final_score:.2f}")
                return True, final_score, "APPROVED"
            else:
                return False, final_score, f"Insufficient consensus: {final_score:.2f}"

        except Exception as e:
            logger.error(f"❌ [Chamber] Critical failure in debate: {e}")
            return False, 0.0, "DEBATE_CRASH"
