CREATE EXTENSION IF NOT EXISTS postgis;


CREATE TABLE IF NOT EXISTS fact_road_disruptions (
    -- Khóa chính tự tăng (Surrogate Key)
    surrogate_key SERIAL PRIMARY KEY,
    
    -- Dữ liệu định danh từ API
    disruption_id VARCHAR(100) UNIQUE NOT NULL,
    category VARCHAR(50),
    severity_level VARCHAR(50),
    
    -- Điểm số mức độ để làm Analytics (1: Minimal, 2: Moderate, 3: Serious)
    severity_score INTEGER,
    
    -- Thông tin địa điểm
    location_name TEXT,
    corridor_name VARCHAR(255),
    comments TEXT,
    
    -- Tọa độ tách rời để dễ truy vấn thường
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    
    -- Tọa độ dạng hình học để làm Geospatial Analysis (PostGIS)
    geom GEOGRAPHY(Point, 4326),
    
    -- Thời gian
    start_at TIMESTAMP WITH TIME ZONE,
    end_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE, -- lastModDateTime từ API
    
    -- Audit fields (Dành cho Data Engineer quản lý pipeline)
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Tạo Index để tăng tốc độ truy vấn theo thời gian và loại sự cố
CREATE INDEX IF NOT EXISTS idx_disruption_start_at ON fact_road_disruptions (start_at);
CREATE INDEX IF NOT EXISTS idx_disruption_category ON fact_road_disruptions (category);
CREATE INDEX IF NOT EXISTS idx_disruption_geom ON fact_road_disruptions USING GIST (geom);