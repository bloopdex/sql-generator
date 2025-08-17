import json
import unittest
from unittest.mock import patch, MagicMock
from src.agents.sql_agent import load_tables, build_messages, generate_sql

class TestSqlAgent(unittest.TestCase):

    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data='{"table1": {"columns": ["col1", "col2"], "meaning": "Table 1 meaning"}}')
    def test_load_tables(self, mock_file):
        tables = load_tables('dummy_path.json')
        expected = {
            "table1": {
                "columns": ["col1", "col2"],
                "meaning": "Table 1 meaning"
            }
        }
        self.assertEqual(tables, expected)
        mock_file.assert_called_once_with('dummy_path.json', 'r', encoding='utf-8')

    def test_build_messages(self):
        tables = {
            "table1": {
                "columns": ["col1", "col2"],
                "meaning": "Table 1 meaning"
            }
        }
        question = "What is the meaning of col1?"
        messages = build_messages(question, tables)
        self.assertEqual(len(messages), 2)
        self.assertIn("You are an expert SQL generator.", messages[0]['content'])
        self.assertIn("QUESTION: " + question, messages[1]['content'])

    @patch('src.agents.sql_agent.OpenAI')
    @patch('src.agents.sql_agent.load_tables', return_value={"table1": {"columns": ["col1", "col2"], "meaning": "Table 1 meaning"}})
    def test_generate_sql(self, mock_load_tables, mock_openai):
        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        mock_client.chat.completions.create.return_value = MagicMock(choices=[MagicMock(message=MagicMock(content="SELECT col1 FROM table1;"))])

        question = "Get col1 from table1"
        sql = generate_sql(question, tables_json_path='dummy_path.json', model='dummy_model', api_key='dummy_key')

        self.assertEqual(sql, "SELECT col1 FROM table1;")
        mock_load_tables.assert_called_once_with('dummy_path.json')
        mock_client.chat.completions.create.assert_called_once()

if __name__ == '__main__':
    unittest.main()