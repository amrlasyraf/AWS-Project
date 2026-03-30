-- Seed 100 Users
INSERT INTO users (user_id, current_age, yearly_income, credit_score)
SELECT 
    gs AS user_id,
    floor(random() * (70-18+1) + 18)::int AS current_age,
    round((random() * 150000 + 30000)::numeric, 2) AS yearly_income,
    floor(random() * (850-300+1) + 300)::int AS credit_score
FROM generate_series(1000, 1099) AS gs
ON CONFLICT (user_id) DO NOTHING;

-- Seed 200 Cards (approx 2 per user)
INSERT INTO cards (card_id, user_id, card_brand, credit_limit, card_on_dark_web)
SELECT 
    (gs * 10) AS card_id,
    1000 + floor(random() * 100)::int AS user_id,
    (ARRAY['Visa', 'Mastercard', 'Amex'])[floor(random() * 3 + 1)] AS card_brand,
    round((random() * 20000 + 5000)::numeric, 2) AS credit_limit,
    (random() > 0.9) AS card_on_dark_web
FROM generate_series(1, 200) AS gs
ON CONFLICT (card_id) DO NOTHING;
