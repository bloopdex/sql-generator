# SQL Generator

This project is a SQL generator that converts natural language questions into SQL queries using a machine learning model. It is designed to help users interact with databases more intuitively by allowing them to ask questions in plain language.

## Project Structure

```
sql-generator
├── src
│   ├── agents
│   │   └── sql_agent.py      # Main logic for generating SQL queries
│   ├── cli.py                 # Command-line interface for the application
│   └── __init__.py            # Marks the directory as a Python package
├── data
│   └── tables.json            # Metadata for the database tables in JSON format
├── tests
│   └── test_sql_agent.py      # Unit tests for the sql_agent.py functionality
├── .env.example                # Example of environment variables for the project
├── requirements.txt            # Lists Python dependencies required for the project
├── pyproject.toml             # Configuration file for Python projects
└── README.md                  # Documentation for the project
```

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd sql-generator
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Set up environment variables by copying `.env.example` to `.env` and filling in the necessary values.

## Usage

To use the SQL generator, you can run the command-line interface with a natural language question. For example:

```
python src/cli.py "What are the names of all employees?"
```

This will generate the corresponding SQL query based on the provided question and the metadata defined in `data/tables.json`.

## Testing

To run the unit tests for the SQL generator, execute the following command:

```
pytest tests/test_sql_agent.py
```

This will ensure that the SQL generation works as expected and that all functions behave correctly.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.