import os
import uuid
import random
from datetime import datetime
import pg8000.native

def lambda_handler(event, context):
    host = os.environ.get('DB_HOST')
    password = os.environ.get('DB_PASSWORD')
    
    con = pg8000.native.Connection(
        host=host, database="postgres", user="postgres",
        password=password, port=5432, timeout=10
    )
    
    try:
        # Fetch valid IDs to ensure Referential Integrity
        valid_users = [row[0] for row in con.run("SELECT user_id FROM users")]
        valid_cards = [row[0] for row in con.run("SELECT card_id FROM cards")]

        batch_size = 20 
        insert_query = """
        INSERT INTO transactions (transaction_id, user_id, card_id, amount, status, transaction_time, use_chip, merchant_id, mcc)
        VALUES (:id, :user, :card, :amt, :status, :time, :chip, :m_id, :mcc)
        """

        for _ in range(batch_size):
            con.run(insert_query, 
                    id=str(uuid.uuid4()), 
                    user=random.choice(valid_users), 
                    card=random.choice(valid_cards), 
                    amt=round(random.uniform(10.0, 500.0), 2), 
                    status='SUCCESS', 
                    time=datetime.now(),
                    chip=random.choice(['Swipe', 'Chip', 'Online']),
                    m_id=random.randint(50000, 60000),
                    mcc=random.randint(1000, 9999))
        
        return {"status": "success", "records_inserted": batch_size}
        
    except Exception as e:
        print(f"Error: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        con.close()
