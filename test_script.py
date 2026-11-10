from src.streaming.database import get_connection, insert_raw_event, insert_prediction, complete_event_processing
import json

conn = get_connection()
payload = json.dumps({"test": True}).encode()

raw_id, inserted = insert_raw_event(conn, "__database_test__", 0, 102, "test-country", payload, "TEST-PREDICTION-001")

cur = conn.cursor()
cur.execute(
    "INSERT INTO public.processed_events (event_id, raw_event_id, processing_status, model_version) VALUES (%s, %s, %s, %s)",
    ("TEST-PREDICTION-001", raw_id, "PROCESSING", "random_forest_test_v1")
)
cur.close()

prediction_id = insert_prediction(conn, "TEST-PREDICTION-001", 7.25, 7.50, "random_forest_test_v1")
complete_event_processing(conn, "TEST-PREDICTION-001", raw_id)

cur = conn.cursor()
cur.execute(
    "SELECT pe.event_id, pe.processing_status, pe.model_version, pe.completed_at IS NOT NULL, re.status, re.is_duplicate, p.predicted_score, p.actual_score, p.absolute_error FROM public.processed_events pe JOIN public.raw_events re ON re.raw_event_id = pe.raw_event_id JOIN public.predictions p ON p.event_id = pe.event_id WHERE pe.event_id = %s",
    ("TEST-PREDICTION-001",)
)
print("Prediction ID:", prediction_id)
print("Resultado:", cur.fetchone())

cur.close()
conn.rollback()
conn.close()