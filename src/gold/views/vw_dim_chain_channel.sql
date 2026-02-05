-- =====================================================================
-- View: gold.vw_dim_chain_channel
-- =====================================================================
-- Business Purpose:
--   Intermediate dimension for chain-channel analysis and surrogate key generation.
--   Enables efficient joins and aggregation by chain/channel.
--
-- Columns:
--   chain_channel_id: Surrogate key (UPPER(chain) | UPPER(channel))
--   chain: Commercial chain (from dim_pdv)
--   channel: Commercial channel (from dim_pdv)
-- =====================================================================

CREATE OR REPLACE VIEW workspace.gold.vw_dim_chain_channel AS
SELECT
  UPPER(chain) || '|' || UPPER(channel) AS chain_channel_id,
  chain,
  channel
FROM workspace.gold.dim_pdv
WHERE chain IS NOT NULL AND channel IS NOT NULL
GROUP BY UPPER(chain), UPPER(channel), chain, channel;
