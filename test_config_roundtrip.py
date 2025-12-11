"""
Test script to verify noise parameters are properly saved and loaded from INI files.
"""
import os
import tempfile
from core.config import Config

def test_noise_params_roundtrip():
    """Test that noise parameters survive save/load cycle."""
    
    # Create config with custom noise parameters
    config1 = Config()
    config1.privacy.noise_level = 0.25
    config1.privacy.cards_jitter = 0.10
    config1.privacy.amount_jitter = 0.08
    config1.privacy.noise_seed = 12345
    
    # Save to temporary INI file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
        temp_path = f.name
    
    try:
        config1.to_ini(temp_path)
        print(f"✓ Saved config to {temp_path}")
        
        # Load from INI file
        config2 = Config.from_ini(temp_path)
        print(f"✓ Loaded config from {temp_path}")
        
        # Verify all noise parameters match
        assert config2.privacy.noise_level == 0.25, \
            f"noise_level mismatch: expected 0.25, got {config2.privacy.noise_level}"
        print(f"✓ noise_level: {config2.privacy.noise_level}")
        
        assert config2.privacy.cards_jitter == 0.10, \
            f"cards_jitter mismatch: expected 0.10, got {config2.privacy.cards_jitter}"
        print(f"✓ cards_jitter: {config2.privacy.cards_jitter}")
        
        assert config2.privacy.amount_jitter == 0.08, \
            f"amount_jitter mismatch: expected 0.08, got {config2.privacy.amount_jitter}"
        print(f"✓ amount_jitter: {config2.privacy.amount_jitter}")
        
        assert config2.privacy.noise_seed == 12345, \
            f"noise_seed mismatch: expected 12345, got {config2.privacy.noise_seed}"
        print(f"✓ noise_seed: {config2.privacy.noise_seed}")
        
        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED - Noise parameters roundtrip successful!")
        print("=" * 60)
        
        # Show the INI file contents for verification
        print("\nINI file contents:")
        print("-" * 60)
        with open(temp_path, 'r') as f:
            contents = f.read()
            # Show only the relevant noise parameter lines
            for line in contents.split('\n'):
                if any(param in line for param in ['noise_level', 'cards_jitter', 'amount_jitter', 'noise_seed']):
                    print(line)
        print("-" * 60)
        
    finally:
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)
            print(f"\n✓ Cleaned up temporary file")

if __name__ == '__main__':
    test_noise_params_roundtrip()

