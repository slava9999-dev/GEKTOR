def compute_final_radar_score(
    vol_spike_s: float, 
    vel_s: float, 
    mom_s: float, 
    volat_s: float,
    orderflow_s: float
) -> int:
    """
    Gerald v3.1 Formula:
    score = 0.30 * volume_spike (max 10)
          + 0.25 * velocity (max 10)
          + 0.20 * momentum (max 10)
          + 0.15 * volatility (max 10)
          + 0.10 * orderflow (max 6)
    """
    raw = (vol_spike_s * 0.30) + (vel_s * 0.25) + (mom_s * 0.20) + (volat_s * 0.15) + (orderflow_s * 0.10)
    # Max possible raw: (10*0.3)+(10*0.25)+(10*0.2)+(10*0.15)+(6*0.1) = 3 + 2.5 + 2 + 1.5 + 0.6 = 9.6
    return int(round(raw * (100.0 / 9.6)))
