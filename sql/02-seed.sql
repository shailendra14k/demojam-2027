-- Demo players, KYC, payment methods, and a few historical transactions.
-- Player IDs are stable so scenarios and the UI can reference them.

INSERT INTO players (player_id, first_name, last_name, country, date_of_birth, email, status, pep_flag, risk_tier, demo_label, registered_at) VALUES
  ('C10281', 'Michael', 'Carter',   'Mexico',        '1984-03-12', 'm.carter.demo@example.com',  'ACTIVE', FALSE, 'HIGH',     'Demo',           now() - interval '410 days'),
  ('C10402', 'Priya',   'Mehta',    'India',         '1991-07-22', 'priya.mehta.demo@example.com','ACTIVE', FALSE, 'MEDIUM',   'Demo',           now() - interval '220 days'),
  ('C10811', 'James',   'Okonkwo',  'Nigeria',       '1979-11-02', 'j.okonkwo.demo@example.com',  'ACTIVE', FALSE, 'HIGH',     'Demo',           now() - interval '88 days'),
  ('C10990', 'Elena',   'Volkov',   'Russia',        '1988-01-30', 'e.volkov.demo@example.com',   'ACTIVE', TRUE,  'CRITICAL', 'PEP Demo',       now() - interval '60 days'),
  ('C11012', 'Daniel',  'Nguyen',   'USA',           '1995-05-18', 'd.nguyen.demo@example.com',   'ACTIVE', FALSE, 'LOW',      'Demo',           now() - interval '500 days'),
  ('C11120', 'Sofia',   'Rossi',    'Italy',         '1990-09-09', 's.rossi.demo@example.com',    'ACTIVE', FALSE, 'LOW',      'Demo',           now() - interval '300 days'),
  ('C11201', 'Chen',    'Wei',      'China',         '1986-02-14', 'chen.wei.demo@example.com',   'ACTIVE', FALSE, 'MEDIUM',   'Demo',           now() - interval '140 days'),
  ('C11330', 'Amira',   'Hassan',   'UAE',           '1993-12-01', 'a.hassan.demo@example.com',   'ACTIVE', FALSE, 'MEDIUM',   'Demo',           now() - interval '95 days'),
  ('C11440', 'Lucas',   'Silva',    'Brazil',        '1982-06-25', 'l.silva.demo@example.com',    'ACTIVE', FALSE, 'HIGH',     'Demo',           now() - interval '45 days'),
  ('C11550', 'Hannah',  'Brooks',   'USA',           '1998-04-04', 'h.brooks.demo@example.com',   'ACTIVE', FALSE, 'LOW',      'Demo',           now() - interval '700 days'),
  ('C11660', 'Yuki',    'Tanaka',   'Japan',         '1987-08-19', 'y.tanaka.demo@example.com',   'ACTIVE', FALSE, 'LOW',      'Demo',           now() - interval '250 days'),
  ('C11770', 'Omar',    'Farouk',   'Egypt',         '1975-10-11', 'o.farouk.demo@example.com',   'ACTIVE', TRUE,  'HIGH',     'PEP Demo',       now() - interval '30 days')
ON CONFLICT (player_id) DO NOTHING;

INSERT INTO player_accounts (account_id, player_id, account_type, status, balance_usd, opened_at)
SELECT 'ACC-' || player_id, player_id, 'PLAYER', 'OPEN', 0, registered_at FROM players
ON CONFLICT (account_id) DO NOTHING;

INSERT INTO kyc_profiles (kyc_id, player_id, kyc_status, id_document_type, nationality, occupation, source_of_funds, risk_rating, watchlist_hit, prior_alerts, last_review_at) VALUES
  ('KYC-C10281', 'C10281', 'VERIFIED', 'PASSPORT',         'Mexico',  'Import/Export',     'Business income',  'HIGH',     FALSE, 4, now() - interval '20 days'),
  ('KYC-C10402', 'C10402', 'VERIFIED', 'PASSPORT',         'India',   'Software engineer', 'Salary',           'MEDIUM',   FALSE, 1, now() - interval '40 days'),
  ('KYC-C10811', 'C10811', 'ENHANCED', 'PASSPORT',         'Nigeria', 'Oil services',      'Business income',  'HIGH',     FALSE, 3, now() - interval '12 days'),
  ('KYC-C10990', 'C10990', 'ENHANCED', 'PASSPORT',         'Russia',  'Consultant',        'Unknown',          'CRITICAL', TRUE,  6, now() - interval '5 days'),
  ('KYC-C11012', 'C11012', 'VERIFIED', 'DRIVERS_LICENSE',  'USA',     'Sales',             'Salary',           'LOW',      FALSE, 0, now() - interval '90 days'),
  ('KYC-C11120', 'C11120', 'VERIFIED', 'PASSPORT',         'Italy',   'Hospitality',       'Salary',           'LOW',      FALSE, 0, now() - interval '60 days'),
  ('KYC-C11201', 'C11201', 'VERIFIED', 'PASSPORT',         'China',   'Trading',           'Business income',  'MEDIUM',   FALSE, 1, now() - interval '18 days'),
  ('KYC-C11330', 'C11330', 'VERIFIED', 'PASSPORT',         'UAE',     'Real estate',       'Family wealth',    'MEDIUM',   FALSE, 2, now() - interval '15 days'),
  ('KYC-C11440', 'C11440', 'PENDING',  'PASSPORT',         'Brazil',  'Entertainment',     'Gaming winnings',  'HIGH',     FALSE, 2, now() - interval '8 days'),
  ('KYC-C11550', 'C11550', 'VERIFIED', 'STATE_ID',         'USA',     'Student',           'Family',           'LOW',      FALSE, 0, now() - interval '120 days'),
  ('KYC-C11660', 'C11660', 'VERIFIED', 'PASSPORT',         'Japan',   'Finance',           'Salary',           'LOW',      FALSE, 0, now() - interval '50 days'),
  ('KYC-C11770', 'C11770', 'ENHANCED', 'PASSPORT',         'Egypt',   'Government liaison','Unknown',          'HIGH',     TRUE,  3, now() - interval '3 days')
ON CONFLICT (kyc_id) DO NOTHING;

INSERT INTO payment_methods (payment_id, player_id, method_type, last4, bank_name, country) VALUES
  ('PM-C10281-1', 'C10281', 'CASH',  NULL,  NULL,                'Mexico'),
  ('PM-C10281-2', 'C10281', 'WIRE',  '8841','Banco Azteca',      'Mexico'),
  ('PM-C10811-1', 'C10811', 'WIRE',  '2201','Zenith Bank',       'Nigeria'),
  ('PM-C10811-2', 'C10811', 'CASH',  NULL,  NULL,                'Nigeria'),
  ('PM-C10990-1', 'C10990', 'WIRE',  '1009','Gazprombank',       'Russia'),
  ('PM-C10990-2', 'C10990', 'CRYPTO',NULL,  NULL,                'Russia'),
  ('PM-C10990-3', 'C10990', 'CASH',  NULL,  NULL,                'Russia'),
  ('PM-C11440-1', 'C11440', 'CASH',  NULL,  NULL,                'Brazil'),
  ('PM-C11012-1', 'C11012', 'CARD',  '4421','Chase',             'USA')
ON CONFLICT (payment_id) DO NOTHING;

INSERT INTO devices (device_id, player_id, device_type, fingerprint) VALUES
  ('DEV-C10281', 'C10281', 'KIOSK',  'fp-carter-01'),
  ('DEV-C11012', 'C11012', 'MOBILE', 'fp-nguyen-01'),
  ('DEV-C10990', 'C10990', 'DESKTOP','fp-volkov-01')
ON CONFLICT (device_id) DO NOTHING;

-- Seed a handful of historical transactions so the dashboard is never empty.
INSERT INTO transactions (transaction_id, player_id, txn_type, amount, location, payment_method, created_at) VALUES
  ('TXN-SEED-001', 'C10281', 'CASH_OUT',  9800,  'Cage 03',     'CASH', now() - interval '55 minutes'),
  ('TXN-SEED-002', 'C10281', 'CASH_OUT',  9500,  'Cage 07',     'CASH', now() - interval '40 minutes'),
  ('TXN-SEED-003', 'C10811', 'WIRE_IN',  25000,  'Bank XYZ',    'WIRE', now() - interval '35 minutes'),
  ('TXN-SEED-004', 'C11012', 'BUY_IN',     400,  'Table 18',    'CHIPS', now() - interval '28 minutes'),
  ('TXN-SEED-005', 'C11120', 'TITO_ISSUE', 220,  'Slot Bank 04','TITO', now() - interval '21 minutes'),
  ('TXN-SEED-006', 'C11550', 'TITO_REDEEM',180,  'Cage 01',     'TITO', now() - interval '18 minutes'),
  ('TXN-SEED-007', 'C10990', 'CASH_IN',  15000,  'Cage 02',     'CASH', now() - interval '12 minutes'),
  ('TXN-SEED-008', 'C11440', 'CASH_OUT',  8900,  'Cage 05',     'CASH', now() - interval '8 minutes')
ON CONFLICT (transaction_id) DO NOTHING;

INSERT INTO wagers (wager_id, player_id, session_id, game_type, amount, win_amount, location, created_at) VALUES
  ('WGR-SEED-001', 'C11012', 'SES-001', 'BLACKJACK', 50, 80, 'Table 18', now() - interval '26 minutes'),
  ('WGR-SEED-002', 'C11120', 'SES-002', 'SLOTS',     20,  0, 'Slot Bank 04', now() - interval '20 minutes')
ON CONFLICT (wager_id) DO NOTHING;

INSERT INTO cage_transactions (cage_txn_id, player_id, direction, amount, cage_window, teller_id, created_at) VALUES
  ('CAG-SEED-001', 'C10281', 'CASH_OUT', 9800, 'Cage 03', 'T-441', now() - interval '55 minutes'),
  ('CAG-SEED-002', 'C10281', 'CASH_OUT', 9500, 'Cage 07', 'T-218', now() - interval '40 minutes'),
  ('CAG-SEED-003', 'C10811', 'WIRE_IN', 25000, 'Wire Desk', 'T-102', now() - interval '35 minutes')
ON CONFLICT (cage_txn_id) DO NOTHING;

-- Baseline pipeline status rows already inserted in 01-init.sql
INSERT INTO aml.player_scores (player_id, player_name, country, risk_score, risk_tier, month_volume, explanation) VALUES
  ('C10281', 'Michael Carter', 'Mexico',  82, 'HIGH',     184200, 'Structuring pattern across multiple cage windows'),
  ('C10811', 'James Okonkwo',  'Nigeria', 76, 'HIGH',     142000, 'Rapid wire-in with limited gaming'),
  ('C10990', 'Elena Volkov',   'Russia',  91, 'CRITICAL', 267500, 'PEP + watchlist + multi-instrument cash cycle'),
  ('C11440', 'Lucas Silva',    'Brazil',  71, 'HIGH',      98000, 'Just-below-threshold cash-outs'),
  ('C10402', 'Priya Mehta',    'India',   44, 'MEDIUM',    31200, 'Elevated velocity, otherwise consistent play'),
  ('C11012', 'Daniel Nguyen',  'USA',     18, 'LOW',       6400,  'Normal recreational play')
ON CONFLICT (player_id) DO NOTHING;

INSERT INTO aml.cases (case_id, player_id, player_name, title, priority, status, assignee, alert_ids) VALUES
  ('CASE-1001', 'C10281', 'Michael Carter', 'Structuring across Cage 03/07', 'HIGH',     'UNDER_REVIEW', 'Sarah Wilson',  '{}'),
  ('CASE-1002', 'C10990', 'Elena Volkov',   'PEP watchlist + multi-rail cash', 'CRITICAL','ASSIGNED',     'Sarah Wilson',  '{}'),
  ('CASE-1003', 'C10811', 'James Okonkwo',  'Wire-in / minimal play',          'HIGH',     'NEW',          NULL,            '{}'),
  ('CASE-1004', 'C11440', 'Lucas Silva',    'Repeat $8.9k cash-outs',          'MEDIUM',   'NEW',          NULL,            '{}')
ON CONFLICT (case_id) DO NOTHING;

INSERT INTO aml.sars (sar_id, case_id, player_id, player_name, status, narrative, amount) VALUES
  ('SAR-9001', 'CASE-1002', 'C10990', 'Elena Volkov',   'PENDING', 'PEP customer cycling cash through cage and wire rails.', 267500),
  ('SAR-9002', 'CASE-1001', 'C10281', 'Michael Carter', 'PENDING', 'Possible structuring: multiple sub-$10k cash-outs.', 184200)
ON CONFLICT (sar_id) DO NOTHING;
