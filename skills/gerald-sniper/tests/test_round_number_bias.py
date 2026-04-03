def test_round_number_bias_is_penalized():
    from core.scoring import calculate_final_score

    class MockRadar:
        rvol = 1.0
        delta_oi_4h_pct = 1.0
        symbol = "BTCUSDT"
        funding_rate = 0.0

    radar = MockRadar()
    btc_ctx = {'trend': 'FLAT', 'hot_sector_coins': []}
    config = {
        'weights': {'level_quality': 30, 'fuel': 30, 'pattern': 25, 'macro': 15},
        'modifiers': {}
    }
    trigger = {'pattern': 'breakout', 'direction': 'LONG'}

    level_round = {
        'price': 100.0,
        'type': 'SUPPORT',
        'source': 'ROUND_NUMBER',
        'touches': 8,
        'distance_pct': 0.5
    }

    level_real = {
        'price': 100.0,
        'type': 'SUPPORT',
        'source': 'WEEK_EXTREME',
        'touches': 8,
        'distance_pct': 0.5
    }

    score_round, bd_round = calculate_final_score(radar, level_round, trigger, btc_ctx, config)
    score_real, bd_real = calculate_final_score(radar, level_real, trigger, btc_ctx, config)

    assert bd_round['level'] < bd_real['level']
