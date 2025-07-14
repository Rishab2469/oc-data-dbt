# Scalable US States Data Vault Architecture

This project implements a scalable Data Vault architecture for processing company registry data from all US states. Instead of creating separate models for each state, we use a parameterized approach with YAML configuration files.

## Architecture Overview

### 1. Configuration-Driven Approach
- **State Configurations**: Each state has a YAML configuration file in `config/states/`
- **Field Mappings**: Define how source fields map to target schema
- **Metadata**: State-specific metadata (jurisdiction codes, source systems, etc.)

### 2. Model Layers

#### Raw Layer (`models/raw/`)
- `stg_us_companies_raw.sql`: Generic raw model that can be parameterized
- State-specific raw models use configuration to define field structure

#### Staging Layer (`models/staging/`)
- `stg_us_companies_structured.sql`: Generic staging model
- Transforms raw data using field mappings from configuration

#### Data Vault Layer (`models/rdv/`)
- `h_companies.sql`: Hub model for companies (using automate_dv package)
- `s_companies.sql`: Satellite model for company attributes
- State-specific models follow the same pattern

## How to Add a New State

### Step 1: Create State Configuration
Create a new YAML file in `config/states/` (e.g., `us_ca.yml` for California):

```yaml
# California State Configuration
state_code: "CA"
jurisdiction_code: "us_ca"
registration_authority_code: "RA000604"
source_system: "ca_secretary_of_state"
source_entity: "companies"

# Field mappings for CA state
field_mappings:
  - source_field: "entity_number"
    target_field: "entity_id"
  - source_field: "entity_name"
    target_field: "current_entity_name"
  # ... add all field mappings

# State-specific fields
state_fields:
  - "entity_number"
  - "entity_name"
  # ... add all available fields

# Data source configuration
data_source:
  url: "https://data.ca.gov/api/views/example/rows.csv?accessType=DOWNLOAD"
  file_format: "CSV"
  encoding: "UTF-8"
  skip_header: 1
```

### Step 2: Create State-Specific Models
Create the following models for your state (replace `ca` with your state code):

#### Raw Model (`models/raw/stg_us_ca_companies_raw.sql`)
```sql
{{ generate_state_raw_model('ca') }}
```

#### Staging Model (`models/staging/stg_us_ca_companies_structured.sql`)
```sql
{{ generate_state_staging_model('ca') }}
```

#### Hub Model (`models/rdv/h_companies_ca.sql`)
```sql
{{ generate_state_hub_model('ca') }}
```

#### Satellite Model (`models/rdv/s_companies_ca.sql`)
```sql
{{ generate_state_satellite_model('ca') }}
```

### Step 3: Run the Models
```bash
# Run for specific state
dbt run --models stg_us_ca_companies_raw stg_us_ca_companies_structured h_companies_ca s_companies_ca

# Or run all models
dbt run
```

## Available Macros

### Configuration Macros
- `load_state_config(state_code)`: Load state configuration from YAML
- `get_state_fields(state_code)`: Get list of fields for a state
- `get_field_mappings(state_code)`: Get field mappings for a state
- `get_state_metadata(state_code)`: Get metadata for a state

### Model Generation Macros
- `generate_state_raw_model(state_code)`: Generate raw model for a state
- `generate_state_staging_model(state_code)`: Generate staging model for a state
- `generate_state_hub_model(state_code)`: Generate hub model for a state
- `generate_state_satellite_model(state_code)`: Generate satellite model for a state

## Benefits of This Approach

1. **Scalability**: Add new states by creating configuration files
2. **Consistency**: All states follow the same Data Vault pattern
3. **Maintainability**: Changes to patterns apply to all states
4. **Flexibility**: Each state can have different field structures
5. **Automation**: Uses automate_dv package for Data Vault patterns

## Example: Adding Texas

1. Create `config/states/us_tx.yml` with Texas-specific configuration
2. Create models using the generation macros
3. Run the models

The system will automatically:
- Load the Texas configuration
- Generate appropriate field mappings
- Create Data Vault models with proper metadata
- Handle state-specific data structures

## Data Vault Patterns Used

- **Hub**: Central entity (companies) with business keys
- **Satellite**: Descriptive attributes of the hub
- **Incremental Loading**: Only process new/changed records
- **Hash Keys**: For efficient joins and change detection

## Package Dependencies

- `Datavault-UK/automate_dv`: For Data Vault pattern implementation
- Version: 0.11.2

## Running the Models

```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Run specific state
dbt run --models stg_us_ny_companies_*

# Test models
dbt test
``` 