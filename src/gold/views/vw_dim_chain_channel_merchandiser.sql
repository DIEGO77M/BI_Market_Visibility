-- =====================================================================
-- View: gold.vw_dim_chain_channel_merchandiser
-- =====================================================================
-- Business Purpose:
--   Intermediate dimension for chain-channel-merchandiser analysis and surrogate key generation.
--   Enables efficient joins and aggregation by chain, channel, and merchandiser.
--
-- Columns:
--   chain_channel_merchandiser_id: Surrogate key (UPPER(chain)|UPPER(channel)|UPPER(merchandiser_code))
--   chain: Commercial chain (from dim_pdv)
--   channel: Commercial channel (from dim_pdv)
--   merchandiser_code: Merchandiser code (from dim_pdv)
-- =====================================================================

CREATE OR REPLACE VIEW workspace.gold.vw_dim_chain_channel_merchandiser AS
SELECT
  UPPER(chain) || '|' || UPPER(channel) || '|' || COALESCE(UPPER(merchandiser_code), 'NA') AS chain_channel_merchandiser_id,
  chain,
  channel,
  merchandiser_code
FROM workspace.gold.dim_pdv
WHERE chain IS NOT NULL AND channel IS NOT NULL
GROUP BY UPPER(chain), UPPER(channel), COALESCE(UPPER(merchandiser_code), 'NA'), chain, channel, merchandiser_code;
