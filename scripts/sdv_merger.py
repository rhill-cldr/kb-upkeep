import json
import pandas as pd
import numpy as np
from datetime import datetime
from pyhocon import ConfigFactory
from sdv.single_table import GaussianCopulaSynthesizer, CTGANSynthesizer
from sdv.metadata import SingleTableMetadata
import warnings
warnings.filterwarnings('ignore')


class SDVSequenceMerger:
    def __init__(self, config_path):
        """Initialize with HOCON configuration file."""
        self.config = ConfigFactory.parse_file(config_path)
        self.reference_df = None
        self.sequences_df = None
        self.synthesizer = None
        
    def load_reference_csv(self):
        """Load reference CSV file."""
        csv_path = self.config.get('reference.csv_path')
        print(f"Loading reference CSV from: {csv_path}")
        self.reference_df = pd.read_csv(csv_path)
        
        # Apply field filters if specified
        excluded_fields = self.config.get('field_mapping.excluded_fields', [])
        if excluded_fields:
            print(f"Excluding fields: {excluded_fields}")
            self.reference_df = self.reference_df.drop(columns=excluded_fields, errors='ignore')
        
        print(f"Reference data shape: {self.reference_df.shape}")
        return self.reference_df
    
    def load_sequences_jsonl(self):
        """Load visit sequences from JSONL file."""
        jsonl_path = self.config.get('sequences.jsonl_path')
        print(f"Loading sequences from: {jsonl_path}")
        
        sequences = []
        with open(jsonl_path) as f:
            for line in f:
                sequences.append(json.loads(line))
        self.sequences_df = pd.DataFrame(sequences)
        print(f"Sequences data shape: {self.sequences_df.shape}")
        return self.sequences_df
    
    def create_sdv_model(self):
        """Create and train SDV model on reference data."""
        model_type = self.config.get('sdv.model_type', 'gaussian_copula')
        
        # Create metadata
        metadata = SingleTableMetadata()
        metadata.detect_from_dataframe(self.reference_df)
        
        # Apply any custom metadata from config
        datetime_columns = self.config.get('field_mapping.datetime_columns', [])
        for col in datetime_columns:
            if col in self.reference_df.columns:
                metadata.update_column(col, sdtype='datetime')
        
        categorical_columns = self.config.get('field_mapping.categorical_columns', [])
        for col in categorical_columns:
            if col in self.reference_df.columns:
                metadata.update_column(col, sdtype='categorical')
        
        # Create synthesizer
        print(f"Creating {model_type} synthesizer...")
        if model_type == 'gaussian_copula':
            self.synthesizer = GaussianCopulaSynthesizer(
                metadata,
                enforce_min_max_values=True,
                default_distribution=self.config.get('sdv.default_distribution', 'norm')
            )
        elif model_type == 'ctgan':
            self.synthesizer = CTGANSynthesizer(
                metadata,
                enforce_min_max_values=True,
                epochs=self.config.get('sdv.epochs', 300)
            )
        else:
            raise ValueError(f"Unknown model type: {model_type}")
        # Fit the model
        print("Training SDV model...")
        self.synthesizer.fit(self.reference_df)
        print("Model training complete")

    def apply_conditional_rules(self, synthetic_df, sequence_row):
         """Apply conditional rules based on sequence data."""
         rules = self.config.get('conditional_rules', {})
         for rule_name, rule_config in rules.items():
             if rule_config.get('enabled', True):
                 condition_field = rule_config.get('condition_field')
                 condition_value = sequence_row.get(condition_field)
                 if condition_field and condition_value:
                     # Apply field mappings based on condition
                     mappings = rule_config.get('field_mappings', {})
                 for target_field, mapping_rules in mappings.items():
                     if target_field in synthetic_df.columns:
                         if condition_value in mapping_rules:
                             synthetic_df[target_field] = mapping_rules[condition_value]
         return synthetic_df
     
    def merge_data(self):
        """Merge sequence data with synthetic fields."""
        print("Generating synthetic data for sequences...")
        # Fields to preserve from original sequences
        preserved_fields = self.config.get('field_mapping.preserved_sequence_fields', [
            'unique_hit_id', 'visit_id', 'hit_timestamp_gmt', 
            'visit_start_timestamp_gmt', 'page_url', 'start_page_url', 
            'visit_number'
        ])
        # Generate synthetic samples
        num_sequences = len(self.sequences_df)
        synthetic_samples = self.synthesizer.sample(num_rows=num_sequences)
        # Start with sequence data
        merged_df = self.sequences_df.copy()
        # Add synthetic fields that aren't in preserved fields
        for col in synthetic_samples.columns:
            if col not in preserved_fields:
                merged_df[col] = synthetic_samples[col]
        # Apply conditional rules for each row
        print("Applying conditional rules...")
        for idx, sequence_row in self.sequences_df.iterrows():
            # Get synthetic row
            synthetic_row = synthetic_samples.iloc[[idx]]
            # Apply rules
            synthetic_row = self.apply_conditional_rules(synthetic_row, sequence_row)
            # Update merged data
            for col in synthetic_row.columns:
                if col not in preserved_fields:
                    merged_df.at[idx, col] = synthetic_row[col].iloc[0]
        # Apply field renaming if specified
        field_renames = self.config.get('field_mapping.rename_fields', {})
        if field_renames:
            merged_df = merged_df.rename(columns=field_renames)
        # Reorder columns if specified
        column_order = self.config.get('output.column_order', [])
        if column_order:
            # Include any columns not in the order at the end
            remaining_cols = [col for col in merged_df.columns if col not in column_order]
            final_order = [col for col in column_order if col in merged_df.columns] + remaining_cols
            merged_df = merged_df[final_order]
        return merged_df
    
    def save_output(self, merged_df):
        """Save merged data to CSV and optionally JSONL."""
        # Save to CSV
        csv_output = self.config.get('output.csv_path', 'merged_sequences.csv')
        merged_df.to_csv(csv_output, index=False)
        print(f"Saved CSV to: {csv_output}")
        # Optionally save to JSONL
        if self.config.get('output.save_jsonl', False):
            jsonl_output = self.config.get('output.jsonl_path', 'merged_sequences.jsonl')
            with open(jsonl_output, 'w') as f:
                for _, row in merged_df.iterrows():
                    f.write(json.dumps(row.to_dict()) + '\n')
            print(f"Saved JSONL to: {jsonl_output}")
    def run(self):
        """Execute the full pipeline."""
        print("Starting SDV Sequence Merger...")
        # Load data
        self.load_reference_csv()
        self.load_sequences_jsonl()
        # Create and train model
        self.create_sdv_model()
        # Merge data
        merged_df = self.merge_data()
        # Save output
        self.save_output(merged_df)
        print(f"Process complete! Generated {len(merged_df)} records with {len(merged_df.columns)} fields")
        # Print sample
        print("\nSample of merged data:")
        print(merged_df.head(3).to_string())


# Example HOCON configuration file content
EXAMPLE_CONFIG = """
# SDV Sequence Merger Configuration

reference {
    csv_path = "reference_data.csv"
}

sequences {
    jsonl_path = "visit_sequences.jsonl"
}

sdv {
    # Model type: gaussian_copula or ctgan
    model_type = "gaussian_copula"
    default_distribution = "gaussian"
    
    # For CTGAN
    epochs = 300
}

field_mapping {
    # Fields to exclude from reference CSV
    excluded_fields = [
        "internal_id",
        "debug_info",
        "deprecated_field"
    ]
    
    # Fields from sequences to preserve (not overwrite)
    preserved_sequence_fields = [
        "unique_hit_id",
        "visit_id", 
        "hit_timestamp_gmt",
        "visit_start_timestamp_gmt",
        "page_url",
        "start_page_url",
        "visit_number"
    ]
    
    # Specify field types for better synthesis
    datetime_columns = [
        "last_update_date",
        "registration_date"
    ]
    
    categorical_columns = [
        "device_type",
        "browser",
        "country",
        "marketing_channel"
    ]
    
    # Rename fields in final output
    rename_fields {
        old_field_name = "new_field_name"
    }
}

conditional_rules {
    # Rules to apply based on sequence data
    page_based_rules {
        enabled = true
        condition_field = "page_url"
        
        field_mappings {
            page_type {
                "https://example.com/" = "homepage"
                "https://example.com/about/" = "about"
                "https://example.com/investors/" = "investors"
            }
            
            content_group {
                "https://example.com/events/" = "events"
                "https://news.example.com/latest/" = "news"
                "https://live.example.com/media/" = "media"
            }
        }
    }
}

output {
    csv_path = "merged_sequences.csv"
    save_jsonl = true
    jsonl_path = "merged_sequences.jsonl"
    
    # Optional: specify column order for output
    column_order = [
        "unique_hit_id",
        "visit_id",
        "hit_timestamp_gmt",
        "page_url",
        "device_type",
        "browser",
        "country"
    ]
}
"""


def create_example_files():
    """Create example configuration and reference CSV files."""
    # Save example config
    with open('merger_config.conf', 'w') as f:
        f.write(EXAMPLE_CONFIG)
    print("Created example configuration file: merger_config.conf")
    
    # Create example reference CSV
    example_data = pd.DataFrame({
        'device_type': ['mobile', 'desktop', 'tablet'] * 10,
        'browser': ['Chrome', 'Safari', 'Firefox', 'Edge'] * 7 + ['Chrome', 'Safari'],
        'country': ['US', 'CA', 'UK', 'US', 'MX'] * 6,
        'marketing_channel': ['organic', 'paid', 'direct', 'social'] * 7 + ['organic', 'paid'],
        'session_duration': np.random.randint(30, 3600, 30),
        'pages_viewed': np.random.randint(1, 50, 30),
        'is_return_visitor': np.random.choice([True, False], 30),
        'internal_id': range(30),
        'debug_info': ['debug_' + str(i) for i in range(30)]
    })
    example_data.to_csv('reference_data.csv', index=False)
    print("Created example reference CSV: reference_data.csv")


def main():
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python sdv_merger.py <config_file>")
        print("\nTo create example files:")
        print("python sdv_merger.py --create-examples")
        return
    
    if sys.argv[1] == '--create-examples':
        create_example_files()
        return
    
    config_file = sys.argv[1]
    merger = SDVSequenceMerger(config_file)
    merger.run()


if __name__ == "__main__":
    main()


