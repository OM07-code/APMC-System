-- APMC Database Schema
-- Run on PostgreSQL initialization

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    role VARCHAR(50) NOT NULL DEFAULT 'buyer', -- farmer, buyer, auctioneer, admin
    phone VARCHAR(50),
    address TEXT,
    kyc_status VARCHAR(50) DEFAULT 'pending',
    kyc_document_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- APMC Locations
CREATE TABLE IF NOT EXISTS apmc_locations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    state VARCHAR(100),
    district VARCHAR(100),
    address TEXT,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Commodities
CREATE TABLE IF NOT EXISTS commodities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    unit VARCHAR(50) DEFAULT 'quintal',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Grades
CREATE TABLE IF NOT EXISTS grades (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    commodity_id UUID NOT NULL REFERENCES commodities(id),
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    quality_params JSONB,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(commodity_id, code)
);

-- Auctions
CREATE TABLE IF NOT EXISTS auctions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    auction_id VARCHAR(100) UNIQUE NOT NULL, -- Human-readable ID like AUC-2024-001
    commodity_id UUID NOT NULL REFERENCES commodities(id),
    grade_id UUID NOT NULL REFERENCES grades(id),
    quantity DECIMAL(15, 3) NOT NULL,
    base_price DECIMAL(15, 2) NOT NULL,
    current_highest_bid DECIMAL(15, 2) DEFAULT 0,
    current_winner_id UUID REFERENCES users(id),
    apmc_location_id UUID NOT NULL REFERENCES apmc_locations(id),
    farmer_id UUID NOT NULL REFERENCES users(id),
    auctioneer_id UUID REFERENCES users(id),
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- pending, active, ended, cancelled
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    duration_seconds INTEGER,
    images TEXT[],
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Bids
CREATE TABLE IF NOT EXISTS bids (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bid_id VARCHAR(100) UNIQUE NOT NULL,
    auction_id UUID NOT NULL REFERENCES auctions(id) ON DELETE CASCADE,
    buyer_id UUID NOT NULL REFERENCES users(id),
    amount DECIMAL(15, 2) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    signature TEXT,
    is_winning BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Price History (for market data)
CREATE TABLE IF NOT EXISTS price_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    commodity_id UUID NOT NULL REFERENCES commodities(id),
    grade_id UUID NOT NULL REFERENCES grades(id),
    apmc_location_id UUID NOT NULL REFERENCES apmc_locations(id),
    price DECIMAL(15, 2) NOT NULL,
    volume DECIMAL(15, 3) DEFAULT 0,
    source VARCHAR(50) NOT NULL, -- auction, manual, external_api
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'
);

-- Market Alerts
CREATE TABLE IF NOT EXISTS market_alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id VARCHAR(100) UNIQUE NOT NULL,
    type VARCHAR(50) NOT NULL,
    commodity_id UUID REFERENCES commodities(id),
    message TEXT NOT NULL,
    metadata JSONB DEFAULT '{}',
    is_read BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Auction Events (Event sourcing)
CREATE TABLE IF NOT EXISTS auction_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id VARCHAR(100) UNIQUE NOT NULL,
    auction_id UUID NOT NULL REFERENCES auctions(id),
    type VARCHAR(50) NOT NULL,
    data JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Peers for WebRTC signaling
CREATE TABLE IF NOT EXISTS peers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    peer_id VARCHAR(100) UNIQUE NOT NULL,
    peer_type VARCHAR(50) NOT NULL, -- auctioneer, buyer, farmer, observer
    apmc_location_id UUID REFERENCES apmc_locations(id),
    user_id UUID REFERENCES users(id),
    metadata JSONB DEFAULT '{}',
    is_online BOOLEAN DEFAULT false,
    last_seen TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_auctions_status ON auctions(status);
CREATE INDEX IF NOT EXISTS idx_auctions_commodity_grade ON auctions(commodity_id, grade_id);
CREATE INDEX IF NOT EXISTS idx_auctions_location ON auctions(apmc_location_id);
CREATE INDEX IF NOT EXISTS idx_auctions_farmer ON auctions(farmer_id);
CREATE INDEX IF NOT EXISTS idx_auctions_time ON auctions(start_time, end_time);
CREATE INDEX IF NOT EXISTS idx_bids_auction ON bids(auction_id);
CREATE INDEX IF NOT EXISTS idx_bids_buyer ON bids(buyer_id);
CREATE INDEX IF NOT EXISTS idx_bids_amount ON bids(amount DESC);
CREATE INDEX IF NOT EXISTS idx_price_history_commodity_grade_location ON price_history(commodity_id, grade_id, apmc_location_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_peers_online ON peers(is_online, apmc_location_id);
CREATE INDEX IF NOT EXISTS idx_auction_events_auction ON auction_events(auction_id, created_at);

-- Insert sample data
INSERT INTO commodities (code, name, category, unit) VALUES
    ('COTTON', 'Raw Cotton', 'Fiber', 'quintal'),
    ('WHEAT', 'Wheat', 'Grain', 'quintal'),
    ('RICE', 'Rice', 'Grain', 'quintal'),
    ('SOYBEAN', 'Soybean', 'Oilseed', 'quintal'),
    ('MAIZE', 'Maize', 'Grain', 'quintal')
ON CONFLICT (code) DO NOTHING;

INSERT INTO grades (commodity_id, code, name, description) 
SELECT c.id, g.code, g.name, g.description
FROM commodities c
CROSS JOIN (VALUES
    ('COTTON', 'A', 'Grade A Cotton', 'Premium quality, staple length > 28mm'),
    ('COTTON', 'B', 'Grade B Cotton', 'Good quality, staple length 25-28mm'),
    ('COTTON', 'C', 'Grade C Cotton', 'Standard quality, staple length 22-25mm'),
    ('WHEAT', 'A', 'Grade A Wheat', 'Protein > 12%, moisture < 12%'),
    ('WHEAT', 'B', 'Grade B Wheat', 'Protein 10-12%, moisture < 13%'),
    ('RICE', 'A', 'Grade A Rice', 'Long grain, < 5% broken'),
    ('RICE', 'B', 'Grade B Rice', 'Medium grain, < 10% broken'),
    ('SOYBEAN', 'A', 'Grade A Soybean', 'Protein > 40%, oil > 19%'),
    ('MAIZE', 'A', 'Grade A Maize', 'Moisture < 14%, aflatoxin < 20ppb')
) g(commodity_code, code, name, description)
WHERE c.code = g.commodity_code
ON CONFLICT (commodity_id, code) DO NOTHING;

INSERT INTO apmc_locations (code, name, state, district, address) VALUES
    ('JLG', 'Jalgaon APMC', 'Maharashtra', 'Jalgaon', 'Market Yard, Jalgaon'),
    ('ZDG', 'Zodage APMC', 'Maharashtra', 'Nashik', 'Market Yard, Zodage'),
    ('NSK', 'Nashik APMC', 'Maharashtra', 'Nashik', 'Market Yard, Nashik'),
    ('PUN', 'Pune APMC', 'Maharashtra', 'Pune', 'Market Yard, Pune'),
    ('NAG', 'Nagpur APMC', 'Maharashtra', 'Nagpur', 'Market Yard, Nagpur')
ON CONFLICT (code) DO NOTHING;

-- Create updated_at trigger
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_auctions_updated_at ON auctions;
CREATE TRIGGER update_auctions_updated_at BEFORE UPDATE ON auctions FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();