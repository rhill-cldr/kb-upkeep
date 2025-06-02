import os
import re
import json
import random
import uuid
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np

HOME_PAGE = os.environ.get("START_PAGE")


class VisitSequenceGenerator:
    def __init__(self, dot_file_path):
        self.graph = defaultdict(list)
        self.start_pages = []
        self.load_graph(dot_file_path)
        
    def load_graph(self, dot_file_path):
        """Load the graph from DOT format file."""
        with open(dot_file_path, 'r') as f:
            content = f.read()
        
        # Extract edges with probabilities
        edge_pattern = r'"([^"]+)"\s*->\s*"([^"]+)"\s*\[label="([^"]+)"\]'
        edges = re.findall(edge_pattern, content)
        
        for source, target, prob in edges:
            self.graph[source].append({
                'target': target,
                'probability': float(prob)
            })
        
        # Identify start pages (pages with no incoming edges)
        all_targets = set()
        all_sources = set(self.graph.keys())
        
        for source in self.graph:
            for edge in self.graph[source]:
                all_targets.add(edge['target'])
        
        # Pages that are sources but not targets are potential start pages
        potential_starts = all_sources - all_targets
        
        # For this specific graph, the home page is the main start
        if HOME_PAGE in potential_starts:
            self.start_pages = [HOME_PAGE]
        else:
            self.start_pages = list(potential_starts) if potential_starts else list(all_sources)
    
    def choose_next_page(self, current_page):
        """Choose next page based on transition probabilities."""
        if current_page not in self.graph:
            return None
        
        edges = self.graph[current_page]
        if not edges:
            return None
        
        # Extract probabilities and targets
        probabilities = [edge['probability'] for edge in edges]
        targets = [edge['target'] for edge in edges]
        
        # Normalize probabilities (in case they don't sum to 1)
        prob_sum = sum(probabilities)
        if prob_sum > 0:
            probabilities = [p / prob_sum for p in probabilities]
        
        # Choose based on probabilities
        return np.random.choice(targets, p=probabilities)
    
    def generate_visit_sequence(self, visitor_id, visit_number, base_timestamp):
        """Generate a single visit sequence for a visitor."""
        sequence = []
        
        # Choose a start page
        start_page = random.choice(self.start_pages)
        current_page = start_page
        
        # Visit start timestamp
        visit_start = base_timestamp
        
        # Generate page views in this visit
        current_timestamp = visit_start
        max_pages_per_visit = random.randint(3, 15)  # Vary visit depth
        
        for page_num in range(max_pages_per_visit):
            # Create hit record
            hit = {
                "unique_hit_id": str(uuid.uuid4()),
                "visit_id": visitor_id,
                "hit_timestamp_gmt": current_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "visit_start_timestamp_gmt": visit_start.strftime("%Y-%m-%d %H:%M:%S"),
                "page_url": current_page,
                "start_page_url": start_page,
                "visit_number": visit_number
            }
            sequence.append(hit)
            
            # Move to next page
            next_page = self.choose_next_page(current_page)
            if next_page is None:
                break
            
            current_page = next_page
            
            # Add time between page views (5 seconds to 3 minutes)
            time_delta = timedelta(seconds=random.randint(5, 180))
            current_timestamp += time_delta
        
        return sequence, current_timestamp
    
    def generate_visitor_sessions(self, num_visitors=100, max_visits_per_visitor=5):
        """Generate multiple visit sequences for multiple visitors."""
        all_sequences = []
        
        # Start date for simulations
        base_date = datetime(2024, 1, 1, 8, 0, 0)
        
        for visitor_num in range(num_visitors):
            visitor_id = str(uuid.uuid4())
            num_visits = random.randint(1, max_visits_per_visitor)
            
            # Starting timestamp for this visitor
            visitor_start = base_date + timedelta(
                days=random.randint(0, 365),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            current_timestamp = visitor_start
            
            for visit_num in range(1, num_visits + 1):
                # Generate a visit sequence
                sequence, end_timestamp = self.generate_visit_sequence(
                    visitor_id, 
                    visit_num, 
                    current_timestamp
                )
                all_sequences.extend(sequence)
                
                # Add gap between visits (1 hour to 7 days)
                if visit_num < num_visits:
                    gap_hours = random.randint(1, 168)
                    current_timestamp = end_timestamp + timedelta(hours=gap_hours)
        
        return all_sequences
    
    def save_to_jsonl(self, sequences, output_path):
        """Save sequences to JSONL format."""
        with open(output_path, 'w') as f:
            for record in sequences:
                f.write(json.dumps(record) + '\n')


def main():
    # Initialize generator
    generator = VisitSequenceGenerator('visit-planning.dot.txt')
    
    # Generate sequences
    print("Generating visitor sequences...")
    sequences = generator.generate_visitor_sessions(
        num_visitors=100,
        max_visits_per_visitor=5
    )
    
    # Save to JSONL
    output_file = 'visit_sequences.jsonl'
    generator.save_to_jsonl(sequences, output_file)
    
    print(f"Generated {len(sequences)} hits across multiple visitor sessions")
    print(f"Saved to {output_file}")
    
    # Print sample statistics
    visitor_counts = defaultdict(int)
    for seq in sequences:
        visitor_counts[seq['visit_id']] += 1
    
    print(f"\nStatistics:")
    print(f"Total unique visitors: {len(visitor_counts)}")
    print(f"Average hits per visitor: {len(sequences) / len(visitor_counts):.2f}")
    
    # Show sample of first few records
    print("\nFirst 3 records:")
    for i, record in enumerate(sequences[:3]):
        print(json.dumps(record, indent=2))


if __name__ == "__main__":
    main()

