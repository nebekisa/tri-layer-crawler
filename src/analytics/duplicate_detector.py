"""
Near-duplicate detection using SimHash.
"""

import hashlib
import logging
from typing import List, Set

logger = logging.getLogger(__name__)


class SimHash:
    """
    SimHash algorithm for near-duplicate detection.
    
    Produces a 64-bit fingerprint of text content.
    Similar texts produce similar hashes (small Hamming distance).
    """
    
    def __init__(self, hash_bits: int = 64):
        """
        Initialize SimHash.
        
        Args:
            hash_bits: Number of bits in hash (64 or 128)
        """
        self.hash_bits = hash_bits
    
    def compute(self, text: str) -> str:
        """
        Compute SimHash fingerprint for text.
        
        Args:
            text: Input text to hash
            
        Returns:
            Hexadecimal string representation of hash
        """
        if not text:
            return '0' * (self.hash_bits // 4)
        
        # Tokenize and weight (simple: word frequency)
        tokens = text.lower().split()
        
        # Count token frequencies
        freq = {}
        for token in tokens:
            freq[token] = freq.get(token, 0) + 1
        
        # Initialize vector
        vector = [0] * self.hash_bits
        
        # Accumulate weighted hashes
        for token, weight in freq.items():
            # Get token hash
            token_hash = int(hashlib.md5(token.encode()).hexdigest(), 16)
            
            # Convert to bits
            for i in range(self.hash_bits):
                if token_hash & (1 << i):
                    vector[i] += weight
                else:
                    vector[i] -= weight
        
        # Convert vector to hash
        simhash = 0
        for i in range(self.hash_bits):
            if vector[i] > 0:
                simhash |= (1 << i)
        
        return format(simhash, f'0{self.hash_bits//4}x')
    
    def hamming_distance(self, hash1: str, hash2: str) -> int:
        """
        Calculate Hamming distance between two SimHashes.
        
        Lower distance = more similar content.
        Distance < 3: Near duplicate
        Distance < 6: Very similar
        Distance < 10: Somewhat similar
        
        Args:
            hash1: First hex hash
            hash2: Second hex hash
            
        Returns:
            Number of differing bits
        """
        if len(hash1) != len(hash2):
            raise ValueError("Hash lengths must match")
        
        # Convert to integers
        int1 = int(hash1, 16)
        int2 = int(hash2, 16)
        
        # XOR and count bits
        xor = int1 ^ int2
        return bin(xor).count('1')


class DuplicateDetector:
    """
    Detect near-duplicate content using SimHash.
    """
    
    def __init__(self, similarity_threshold: int = 3):
        """
        Initialize duplicate detector.
        
        Args:
            similarity_threshold: Max Hamming distance for duplicates
        """
        self.simhash = SimHash()
        self.threshold = similarity_threshold
        self._known_hashes: Set[str] = set()
    
    def compute_hash(self, text: str) -> str:
        """Compute SimHash for text."""
        return self.simhash.compute(text)
    
    def is_duplicate(self, text: str, known_texts: List[str] = None) -> tuple:
        """
        Check if text is a duplicate of known content.
        
        Args:
            text: Text to check
            known_texts: List of known texts to compare against
            
        Returns:
            (is_duplicate: bool, closest_distance: int, matching_hash: str)
        """
        text_hash = self.compute_hash(text)
        
        # Check against stored hashes
        for known_hash in self._known_hashes:
            distance = self.simhash.hamming_distance(text_hash, known_hash)
            if distance <= self.threshold:
                return True, distance, known_hash
        
        # Check against provided texts
        if known_texts:
            for known_text in known_texts:
                known_hash = self.compute_hash(known_text)
                distance = self.simhash.hamming_distance(text_hash, known_hash)
                if distance <= self.threshold:
                    return True, distance, known_hash
        
        return False, 64, ""
    
    def add_hash(self, hash_value: str) -> None:
        """Add a hash to the known set."""
        self._known_hashes.add(hash_value)
    
    def add_text(self, text: str) -> str:
        """
        Add text to known set and return its hash.
        
        Args:
            text: Text to add
            
        Returns:
            SimHash of the text
        """
        hash_value = self.compute_hash(text)
        self._known_hashes.add(hash_value)
        return hash_value
    
    def similarity_score(self, text1: str, text2: str) -> float:
        """
        Calculate similarity score between two texts (0.0 to 1.0).
        
        Args:
            text1: First text
            text2: Second text
            
        Returns:
            Similarity score (1.0 = identical, 0.0 = completely different)
        """
        hash1 = self.compute_hash(text1)
        hash2 = self.compute_hash(text2)
        distance = self.simhash.hamming_distance(hash1, hash2)
        
        # Convert distance to similarity score
        max_distance = 64
        return 1.0 - (distance / max_distance)