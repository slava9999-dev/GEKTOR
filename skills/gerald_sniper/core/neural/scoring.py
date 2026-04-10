import numpy as np

class LSTMPredictorMock:
    def predict(self, velocity_series):
        if not velocity_series or len(velocity_series) == 0:
            return 0.5
        features = np.array(velocity_series)
        momentum = np.mean(np.diff(features)) if len(features) > 1 else np.mean(features)
        # Sigmoid activation mapping momentum to 0-1
        return 1 / (1 + np.exp(-momentum * 10))

class TransformerDetectorMock:
    def predict(self, volume_profile):
        if not volume_profile or len(volume_profile) == 0:
            return 0.5
        v = np.array(volume_profile)
        # Softmax based mock attention calculation
        attention = np.exp(v - np.max(v)) / np.sum(np.exp(v - np.max(v))) 
        weighted_vol = np.sum(v * attention)
        return min(max(weighted_vol / 5.0, 0.0), 1.0)
        
class CNNClassifierMock:
    def predict(self, price_action):
        if not price_action or len(price_action) == 0:
            return 0.5
        kernel = np.array([0.2, 0.5, 0.2])
        if len(price_action) >= 3:
            convolved = np.convolve(price_action[-10:], kernel, mode='valid')
            return 1 / (1 + np.exp(-np.mean(convolved))) # Sigmoid on convolved features
        return 0.5

class AdaptiveDetectorWeights:
    def __init__(self):
        self.performance = {
            'breakout': {'success': 0, 'total': 0, 'weight': 1.0},
            'volume': {'success': 0, 'total': 0, 'weight': 1.0},
            'squeeze': {'success': 0, 'total': 0, 'weight': 1.0},
            'compression': {'success': 0, 'total': 0, 'weight': 1.0}
        }
    
    def update_weights(self, detector_name: str, was_profitable: bool):
        if detector_name not in self.performance:
            return
        self.performance[detector_name]['total'] += 1
        if was_profitable:
            self.performance[detector_name]['success'] += 1
            
        # Success rate -> Adaptive weight (Base 1.0, scales to 1.5 for highly successful, down to 0.5 for failing)
        success_rate = self.performance[detector_name]['success'] / max(1, self.performance[detector_name]['total'])
        self.performance[detector_name]['weight'] = max(0.5, success_rate * 1.5)

adaptive_weights = AdaptiveDetectorWeights()

class BERTSentimentMock:
    def predict(self, sentiment_score):
        # Raw market_sentiment.score (-100 to 100)
        norm = sentiment_score / 100.0
        return 1 / (1 + np.exp(-norm * 2))

class NeuralSignalProcessor:
    def __init__(self):
        self.models = {
            'velocity_predictor': LSTMPredictorMock(),
            'volume_anomaly': TransformerDetectorMock(),
            'momentum_classifier': CNNClassifierMock(),
            'sentiment_fusion': BERTSentimentMock()
        }
        
    def predict_signal_probability(self, market_data, sentiment_score=0, trigger_type=None):
        # Extract sequences
        velocity_series = [c['close'] - c['open'] for c in market_data.get('m5', [])[-20:]]
        volume_profile = [v['volume'] for v in market_data.get('m5', [])[-10:]]
        price_action = [c['close'] for c in market_data.get('m5', [])[-15:]]
        
        velocity_prob = self.models['velocity_predictor'].predict(velocity_series)
        volume_prob = self.models['volume_anomaly'].predict(volume_profile)
        momentum_prob = self.models['momentum_classifier'].predict(price_action)
        sentiment_prob = self.models['sentiment_fusion'].predict(sentiment_score)
        
        # Volatility
        volatility = market_data.get('radar', None)
        current_vol = getattr(volatility, 'atr_pct', 2.0) if volatility else 2.0
        
        # Ensemble weighting based on current market condition
        base_weights = np.array([0.25, 0.25, 0.3, 0.2])
        if current_vol > 4.0:
            base_weights = np.array([0.2, 0.35, 0.35, 0.1])
        elif current_vol < 1.5:
            base_weights = np.array([0.35, 0.15, 0.2, 0.3])
            
        probs = np.array([velocity_prob, volume_prob, momentum_prob, sentiment_prob])
        ensemble_prob = np.sum(probs * base_weights)
        
        # Apply Adaptive Detector Booster
        if trigger_type:
            detector_weight = adaptive_weights.performance.get(trigger_type, {}).get('weight', 1.0)
            # Boost base probability if this trigger historically succeeds
            ensemble_prob = min(1.0, ensemble_prob * detector_weight)
        
        return float(ensemble_prob)

neural_processor = NeuralSignalProcessor()
