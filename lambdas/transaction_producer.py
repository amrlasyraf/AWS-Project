import os
import uuid
import random
from datetime import datetime, timedelta
import pg8000.native

# Modular Configuration
PARTNERS_CONFIG = {
    'PARTNER_A': {'suffix': ''},
    'PARTNER_B': {'suffix': '_partner_b'}
}

def lambda_handler(event, context):
    host = os.environ.get('DB_HOST')
    password = os.environ.get('DB_PASSWORD')
    
    con = pg8000.native.Connection(
        host=host, database="postgres", user="postgres",
        password=password, port=5432, timeout=10
    )
    
    try:
        # Task 1: Select partner from metadata configuration
        partner_id = random.choice(list(PARTNERS_CONFIG.keys()))
        config = PARTNERS_CONFIG[partner_id]
        suffix = config['suffix']
        
        # Map tables dynamically
        tables = {
            'transactions': f"transactions{suffix}",
            'users': f"users{suffix}",
            'cards': f"cards{suffix}"
        }

        # Fetch valid users ONCE outside the loop
        valid_users = [row[0] for row in con.run(f"SELECT user_id FROM {tables['users']}")]

        batch_size = 20 
        insert_query = f"""
        INSERT INTO {tables['transactions']} (
            transaction_id, user_id, card_id, amount, status, 
            transaction_time, updated_at, use_chip, merchant_id, merchant_name, mcc, risk_score
        )
        VALUES (:id, :user, :card, :amt, :status, :time, :updated, :chip, :m_id, :m_name, :mcc, :risk)
        """

        for _ in range(batch_size):
            t_id = str(uuid.uuid4())
            u_id = random.choice(valid_users)
            
            # FIX: Fetch a card that actually belongs to this specific user
            c_id = con.run(f"SELECT card_id FROM {tables['cards']} WHERE user_id = {u_id} LIMIT 1")[0][0]
            
            amt = round(random.uniform(10.0, 500.0), 2)
            created_time = datetime.now()
            
            # Shared metadata for this specific transaction
            m_id = random.randint(50000, 60000)
            m_name = f"Merchant_{m_id}"
            mcc = random.randint(1000, 9999)
            chip = random.choice(['Swipe', 'Chip', 'Online'])
            risk = round(random.uniform(0.01, 0.99), 2)

            # 3-Step Lifecycle: PENDING -> APPROVED -> (SUCCESS or FAILED)
            lifecycle = [
                ('PENDING', created_time),
                ('APPROVED', created_time + timedelta(seconds=2)),
                (random.choice(['SUCCESS', 'FAILED']), created_time + timedelta(seconds=7))
            ]

            for status, updated_at in lifecycle:
                con.run(insert_query, 
                        id=t_id, user=u_id, card=c_id, amt=amt, 
                        status=status, time=created_time, updated=updated_at,
                        chip=chip, m_id=m_id, m_name=m_name, mcc=mcc, risk=risk)
        
        return {
            "status": "success", 
            "partner": partner_id,
            "transactions_processed": batch_size, 
            "total_records": batch_size * 3
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        con.close()