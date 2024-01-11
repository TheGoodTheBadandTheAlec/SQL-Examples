#####Update Base Data

import pandas as pd
import cryptocompare as cc
import concurrent.futures
import re
import requests
from datetime import datetime
import cryptocmd
from cryptocmd import CmcScraper
from concurrent.futures import ThreadPoolExecutor

# Your list of symbols
symbols = ['BTC', 'LTC', 'DASH', 'XMR', 'NXT', 'ETC', 'DOGE', 'ZEC', 'BTS', 'DGB', 'XRP', 'PPC', 'GEO', 'ACOIN', 'BLK', 'CSC', 'XVG', 'EMC2', 'FTC', 'GLC', 'IOC', 'NAV', 'NMC', 'POT', 'XLM', 'SYS', 'VIA', 'VRC', 'VTC', 'ZED', 'BCN', 'XDN', 'XEM', 'SIGNA', 'XCP', 'MAID', 'MONA', 'UFOCOIN', 'PINK', 'XBC', 'RDN', 'BSTY', 'NXS', 'SPHR', 'CRW', 'CHIP', 'EXP', 'GRS', 'SC', 'VAL', 'EMC', 'DCR', 'REP', 'OK', 'DGD', 'LSK', 'WAVES', 'CAB', 'STEEM', 'BLRY', 'XWC', 'SLS', '404', 'BOLI', 'CLUD', 'ZOOM', 'YOVI', 'COX', 'BNT', 'BIT16', 'OMNI', 'LBC', 'STRAX', 'SNGLS', 'XAUR', 'ICN', 'WINGS', 'KMD', 'NEO', 'ZET2', 'BXT', 'ZNE', 'COVAL', 'ARM', 'BTCR', 'BRONZ', 'BBCC', 'THC', 'RRT', 'FIRO', 'ARDR', 'BS', 'ZUR', '32BIT', 'GBYTE', '2BACCO', 'CHOOF', 'ARK', 'GLM', 'ALC', '365', 'BRDD', 'TIME', 'CCXC', 'BSTAR', 'ANAL', 'AV', 'ASAFE2', 'MKR', 'YMC', 'PIVX', 'PLU', 'BTZ', 'UBQ', 'PAC', 'MLN', 'PAY', 'ACES', 'BIOB', 'SWT', 'ZER', 'CHAT', 'EDG', 'CJC', 'BLAZR', 'RLC', 'GNO', 'LUN', 'TKN', 'HMQ', 'PZM', 'ANT', 'BITOK', 'BAT', 'QTUM', 'ZEN', 'MIOTA', 'QRL', 'PTOY', 'MYST', 'SNM', 'SNT', 'AVT', 'CVC', 'DENT', 'VGX', 'XTZ', 'EOS', 'MCO', 'NMR', 'ADX', 'USDT', 'NANO', 'NIM', 'SSV', 'DNT', 'SAN', 'KIN', 'FUN', 'MTL', 'HVN', 'PPT', 'SNC', 'OAX', 'LA', 'XUC', 'PLR', 'PRO', 'SYC', 'ZRX', 'PRE', 'STORJ', 'OMG', 'POE', 'AE', 'PART', 'AGRS', 'DMT', 'ADS', 'VIB', 'ATOM', 'MANA', 'SMART', 'CRM', 'BCH', 'MOBI', 'STOX', 'BNB', 'MTH', 'GAS', 'FIL', 'BTM', 'DCN', 'DLT', 'SUB', 'NEBL', 'IGNIS', 'EVX', 'VET', 'UTK', 'AMB', 'WAN', 'NYC', 'PST', 'MTX', 'ENJ', 'CNX', 'ETHER', 'WTC', 'ORME', 'CAPP', 'VEE', 'RCN', 'LINK', 'PNT', 'KNC', 'TRX', 'SALT', 'LRC', 'CAS', 'ADA', 'BIS', 'ICX', 'BTCZ', 'ETP', 'ERT', 'REQ', 'VIBE', 'AST', 'FLP', 'CND', 'TZC', 'ENG', 'WAXP', 'POWR', 'MDA', 'ZSC', 'INXT', 'REV', 'BITCM', 'CPAY', 'RUP', 'APPC', 'BTG', 'RNDR', 'KCS', 'DRGN', 'KEY', 'DOV', 'ETN', 'ALTCOM', 'OST', 'DATA', 'BCPT', 'NULS', 'PHORE', 'SRN', 'BNK', 'QSP', 'QASH', 'XLQ', 'BCD', 'UQC', 'LEND', 'ABYSS', 'BDG', 'BTCM', 'AION', 'COFI', 'ACT', 'SEND', 'FYP', 'NGC', 'SBTC', 'SMT', 'BNTY', 'ELF', 'DBC', 'UTT', 'GET', 'HTML', 'GNX', 'NAS', 'REM', 'SHND', 'TSL', 'PMA', 'MDS', 'INK', 'HPB', 'KZC', 'TEL', 'ZAP', 'EKO', 'BTO', 'IDEX', 'IDH', 'AGI', 'BFT', 'CRPT', 'OCN', 'THETA', 'MDT', 'IOST', 'TCT', 'TRAC', 'ZIL', 'QBAO', 'SWFTC', 'SENT', 'IPL', 'AXPR', 'DDD', 'CPC', 'NPXS', 'ZPT', 'MAN', 'BCDN', 'POLY', 'RUFF', 'ELA', 'BLZ', 'SWM', 'MDCL', 'BIX', 'ABT', 'LYM', 'FSN', 'SAI', 'HT', 'CTC', 'TOKC', 'SHPING', 'LCC', 'TONE', 'BAX', 'REN', 'AUC', 'RVN', 'XYO', 'RFR', 'PROPS', 'CEL', 'CRDTS', 'ONT', 'CLO', 'DATX', 'SNX', 'TOMO', 'XAYA', 'INSTAR', 'CHP', 'IHT', 'BTRM', 'ELEC', 'LOOM', 'PAN', 'GO', 'ADK', 'ESS', 'TUSD', 'MITH', 'SWTH', 'FDZ', 'CEEK', 'EOSDAC', 'MEDIC', 'CTXC', 'ZEL', 'DEV', 'SENC', 'NCT', 'UUU', 'XHV', 'DOCK', 'SIG', 'XMC', 'HOT', 'GEM', 'TIPS', 'PENTA', 'PROTON', 'DERO', 'KRL', 'NEXO', 'MITX', 'CVT', 'XBI', 'VRA', 'UBT', 'LBA', 'SKM', 'SHR', 'UBEX', 'KEC', 'OPEN', 'LNKC', 'MNW', 'IOTX', 'SNTR', 'NKN', 'QKC', 'BMX', 'SOUL', 'OXEN', 'CBC', 'PI', 'RAISE', 'UPP', 'IQN', 'EPIK', 'SPARTA', 'PNY', 'SAFE', 'WORX', 'VIDT', 'MET', 'CET', 'ZCN', 'LET', 'CPT', 'HSC', 'LRN', 'OLT', 'IQ', 'XMX', 'VITE', 'XDNA', 'RPL', 'CENNZ', 'FREE', 'XAP', 'NOIA', 'OMI', 'APL', 'ENQ', 'DAG', 'OKB', 'MODEX', 'EURS', 'BOX', 'COTI', 'KAN', 'HIT', 'PASS', 'DGTX', 'DPY', 'MIB', 'GUSD', 'DAV', 'NHCT', 'IAG', 'USDC', 'ONGAS', 'XCASH', 'USDP', 'DIVI', 'HC', 'VTHO', 'BEAT', 'DFXT', 'BF', 'BCDT', 'ETHO', 'ABBC', 'CATT', 'VEX', 'F2K', 'LPT', 'BSV', 'WOM', 'COS', 'NEW', 'BTZC', 'XELS', 'NRG', 'DSLA', 'SYLO', 'META', 'AVA', 'SUSD', 'MVL', 'FTM', 'RBTC', 'PIRATE', 'AERGO', 'GARD', 'VIDY', 'GRIN', 'WETH', 'BEAM', 'RIF', 'DUSK', 'BTT', 'HEDG', 'WBTC', 'BTU', 'QNT', 'ASD', 'LTO', 'DRF', 'VEO', 'FET', 'KUV', 'USDS', 'ECTE', 'ANKR', 'SHA', 'CREDIT', 'CRO', 'DIO', 'VSYS', 'ZEON', 'CELR', 'TFUEL', 'LEVL', 'UGAS', 'OOKI', 'ORBS', 'RFOX', 'GTH', 'SOLVE', 'BOLT', 'DARC', 'MATIC', 'TT', 'LAMB', 'DREP', 'TMN', 'FX', 'DAPPT', 'GNTO', 'OCEAN', 'ARRR', 'BCX', 'LEO', 'AYA', 'RSR', 'ONE', 'MOC', 'VBK', 'YCC', 'MIX', 'CHR', 'BORA', 'DX', 'MTV', 'TRIAS', 'VNT', 'WXT', 'KAT', 'BEST', 'HYDRO', 'ATP', 'WICC', 'BIHU', 'TOPN', 'STPT', 'SNET', 'IRIS', 'BTCB', 'FTI', 'DOS', 'ALGO', 'MIN', 'MBL', 'GSE', 'MXC', 'NBOT', 'RATING', 'CNNS', 'AMPL', 'ROOBEE', 'SOP', 'LEMO', 'TYPE', 'GNY', 'SRK', 'TOKO', 'GRN', 'SHX', 'UPX', 'ARPA', 'HXRO', 'WIN', 'NUT', 'VRSC', 'LUNC', 'FTT', 'GMAT', 'PERL', 'SIX', 'LOCUS', 'SERO', 'EM', 'TSHP', 'SXP', 'FRM', 'GT', 'CHZ', 'SINS', 'BDX', 'COMBO', 'AKRO', 'AMON', 'XCHF', 'DVP', 'FOR', 'KSC', 'LKN', 'PIB', 'PNK', 'PROM', 'ULTRA', 'ZAIF', 'NYE', 'FO', 'BAND', 'HBAR', 'BUSD', 'WIKEN', 'MCC', 'PAXG', 'TRP', 'IDRT', 'MX', 'BGONE', 'TLOS', 'LBK', 'ME', 'DMS', 'STX', 'KAVA', 'CKB', 'DAI', 'XDGB', 'UNICORN', 'SUTER', 'FCT', 'VLX', 'RUNE', 'DILI', 'MCH', 'LOBS', 'DAD', 'EUM', 'EOSC', 'KLAY', 'TROY', 'XFC', 'USDN', 'BRZ', 'EVY', 'KSM', 'CNTM', 'SCAP', 'QTCON', 'XDC', 'DMTC', 'TRB', 'XSPC', 'OXT', 'MAP', 'BOA', 'CETH', 'XTP', 'ZYN', 'OGN', 'BEPRO', 'EXM', 'HEX', 'KOK', 'APM', 'MESH', 'WIKI', 'PCI', 'GOD', 'ALY', 'KDA', 'LBXC', 'JOB', 'BTBL', 'CCX', 'SYM', 'G1X', 'ZOC', 'YTN', 'AREPA', 'CHEESE', 'WRX', 'AEVO', 'EGEM', 'DXO', 'GOSS', 'GIO', 'TELOS', 'SIERRA', 'VIVID', 'RPD', 'MERI', 'NOR', 'X42', 'XWP', 'HNS', 'VEGA', 'NWC', 'XAUT', 'NAX', 'DAPP', 'SOLO', 'ERG', 'TRYB', 'HBD', 'USDJ', 'NII', 'EWT', 'MORE', 'EURT', 'LCX', 'BTCV', 'HIVE', 'COSP', 'HUNT', 'SOL', 'DEP', 'HMR', 'SNB', 'PCX', 'CTSI', 'XXA', 'SENSO', 'OBSR', 'JST', 'ZNZ', 'NYZO', 'TNC', 'PXP', 'TYC', 'JUP', 'TWT', 'LYXE', 'METAC', 'LOON', 'BAN', 'XPR', 'STMX', 'AR', 'ASM', 'HIBS', 'DKA', 'DRM', 'PHNX', 'WGRT', 'COMP', 'UMA', 'CELO', 'BIDR', 'BAL', 'DAWN', 'BTSE', 'DOT', 'ARX', 'ISP', 'KAI', 'TRCL', 'AVAX', 'SWAP', 'KEEP', 'WEST', 'FIO', 'DEXT', 'ALEPH', 'MTA', 'ORN', 'DFI', 'YFI', 'SWINGBY', 'VXV', 'XOR', 'HNT', 'NEST', 'RING', 'DDRT', 'MCB', 'WNXM', 'CNS', 'KTON', 'SUKU', 'DIA', 'DF', 'MLK', 'CREAM', 'GEEQ', 'DPIE', 'XRT', 'SRM', 'YAM', 'EDGEW', 'CRV', 'SAND', 'OM', 'PRQ', 'BLY', 'RENBTC', '4ART', 'YFII', 'GHT', 'REAP', 'LAYER', 'PSG', 'KLP', 'SUSHI', 'LIEN', 'KLV', 'PEARL', 'TAI', 'EGLD', 'HAI', 'USTC', 'CORN', 'SALMON', 'JFI', 'BEL', 'ADEL', 'MATH', 'CELOUSD', 'SWRV', 'AMP', 'HGET', 'PHA', 'CRT', 'FUND', 'CRP', 'ACH', 'GOF', 'FRONT', 'WING', 'PICKLE', 'SASHIMI', 'SAKE', 'DPI', 'GHST', 'REVV', 'DEGO', 'UNI', 'DBOX', 'RARI', 'NSBT', 'NBS', 'GALA', 'LGCY', 'RIO', 'DHT', 'SHROOM', 'FLM', 'PTF', 'TON', 'MARO', 'VELO', 'BURGER', 'BAKE', 'MINI', 'RFUEL', 'ONIT', 'TITAN', 'AGS', 'CRU', 'POLS', 'CAKE', 'MXT', 'AHT', 'SFG', 'VALUE', 'AAVE', 'XVS', 'ALPHA', 'SCRT', 'WISE', 'AXEL', 'DODO', 'CVP', 'TRIX', 'INJ', 'PLA', 'ECELL', 'EVER', 'NEAR', 'OCTO', 'EZ', 'UFT', 'AKT', 'ZEE', 'ATRI', 'TBTC', 'FIS', 'HMT', 'RAMP', 'FARM', 'AUDIO', 'CTK', 'KP3R', 'PERP', 'SLP', 'WOO', 'ROSE', 'WEMIX', 'XPRT', 'HARD', 'ORAI', 'AXS', 'DVI', 'ORC', 'XEC', 'UNFI', 'HEZ', 'LINA', 'SFI', 'CFX', 'HEGIC', 'UBX', 'KFC', 'SPA', 'API3', 'SKL', 'GALATA', 'MED', 'WOZX', 'MIR', 'COVER', 'ROOK', 'CTI', 'BADGER', 'XCUR', 'FLR', 'YLD', 'FRA', 'GRT', 'REEF', 'TRA', 'BOND', 'POND', 'JUV', 'TVK', 'ADP', 'ASR', 'RLY', 'DYP', 'STETH', '1INCH', 'ATM', 'OG', 'LON', 'FRAX', 'FXS', 'MIS', 'XED', 'NFTX', 'MSWAP', 'HYPE', 'LDO', 'CUDOS', 'HTR', 'BTCST', 'OKT', 'TRU', 'XSGD', 'NORD', 'MASS', 'DIGG', 'PBR', 'YFDAI', 'SX', 'FLOW', 'BETH', 'LIT', 'XTM', 'SFP', 'BMI', 'CWS', 'GUM', 'XNO', 'INDEX', 'ETH2', 'DAO', 'GOVI', 'STRONG', 'AUCTION', 'MDX', 'EVEREST', 'QUICKOLD', 'MOD', 'VSP', 'TORN', 'RAI', 'RAY', 'QTF', 'BONDLY', 'LPOOL', 'WHALE', 'DG', 'MUSE', 'MONAV', 'POLK', 'POLC', 'ACM', 'MASK', 'EGG', 'HOPR', 'ALCX', 'MATTER', 'TOWER', 'BSCPAD', 'STUDENTC', 'ALPACA', 'WMT', 'SUPER', 'BDP', 'ETHERNITY', 'YIELD', 'CGG', 'HAPI', 'ALICE', 'RAD', 'HOGE', 'BIFI', 'OXY', 'BCUG', 'PAINT', 'ANC', 'XYM', 'DORA', 'INV', 'TARA', 'WOOP', 'CAT', 'GLQ', 'PUNDIX', 'FST', 'ILV', 'POPSICLE', 'AIOZ', 'MOB', 'PMON', 'AUTO', 'MCO2', 'LQTY', 'LUSD', 'STRK', 'CERE', 'PUSH', 'TKO', 'GMEE', 'TLM', 'RBC', 'MBOX', 'GHX', 'FIDA', 'BOSON', 'PYR', 'ZIG', 'SHIB', 'MINA', 'LEASH', 'KISHU', 'DHV', 'FORTH', 'ELON', 'VR', 'FLX', 'HORD', 'HAKA', 'INSUR', 'FEI', 'PENDLE', 'STND', 'TCP', 'NAOS', 'LOCG', 'XCH', 'CTX', 'FLUX', 'LABS', 'ACE', 'SOMNIUM', 'POLX', 'ICP', 'GYEN', 'BIFIF', 'FCL', 'GDT', 'CSPR', 'CVX', 'HOTCROSS', 'KUB', 'MEDIA', 'GTC', 'NFT', 'O3', 'CHEX', 'FLY', 'OUSD', 'XCAD', 'SPELL', 'BLKC', 'CAPS', 'SUN', 'ATA', 'VEED', 'LSS', 'IOI', 'MULTIV', 'QUARASHI', 'ANV', 'FORM', 'PLANETS', 'CQT', 'INST', 'DPX', 'FLOKI', 'XAVA', 'GENS', 'TRIBE', 'STARL', 'STEP', 'QRDO', 'OOE', 'CFG', 'WCFG', 'CLV', 'KAR', 'C98', 'SPS', 'YGG', 'BORING', 'DERC', 'EFI', 'MIM', 'TOKE', 'SAITAMA', 'BITCI', 'DFA', 'MPL', 'HI', 'EQX', 'WNCG', 'BIT', 'NGL', 'SKU', 'FB', 'XRD', 'KALYCOIN', 'PNG', 'QI', 'KUMA', 'DPET', 'MOVR', 'CYCE', 'JASMY', 'EDEN', 'SDN', 'DDX', 'AGLD', 'HERO', 'DYDX', 'BTRST', 'ATLAS', 'SDAO', 'PBX', 'XNL', 'KLO', 'RMRK', 'PIT', 'BETA', 'OPUL', 'ARV', 'POLIS', 'RARE', 'WSIENNA', 'WTK', 'BLOK', 'FOX', 'AKTIO', 'FODL', 'EDGT', 'GAFI', 'SBR', 'JOE', 'METIS', 'EVRY', 'LAZIO', 'PARA', 'RBN', 'ENS', 'TUP', 'SCLP', 'SGB', 'SAMO', 'CATGIRL', 'TABOO', 'AURY', 'GODS', 'DAR', 'CHESS', 'IMX', 'BNC', 'MC', 'BNX', 'CELL', 'DREAMS', 'NOTE', 'CWAR', 'SLND', 'WILD', 'XDEFI', 'BOBA', 'ANGLE', 'DVF', 'STRP', 'UFO', 'PSP', 'PORTO', 'GENE', 'GMCOIN', 'PTU', 'SIS', 'BICO', 'SANTOS', 'XTAG', 'KILT', 'DEXA', 'TRL', 'PEOPLE', 'TONCOIN', 'VVS', 'DONK', 'VOXEL', 'UNB', 'TRVL', 'DESO', 'GOG', 'SFM', 'HIGH', 'WSB', 'GF', 'DEVT', 'AAA', 'MBS', 'DFL', 'MNGO', 'MSOL', 'SYN', 'VPAD', 'GAMMA', 'REVU', 'GLMR', 'GFI', 'KINT', 'AURORA', 'SLC', 'POKT', 'ZAM', 'LOKA', 'NUM', 'TRADE', 'LOOKS', 'OSMO', 'ABEY', 'ACA', 'ASTR', 'LOVE', 'GARI', 'CHMB', 'DMTR', 'FCON', 'GMTT', 'ARKER', 'FRR', 'FTG', 'PEL', 'SOLR', 'CLH', 'CREDI', 'DAPPX', 'CPOOL', 'ORCA', 'B2M', 'CIND', 'TONIC', 'SNS', 'HBB', 'ATS', 'ZKP', 'TRACE', 'NT', 'WALLET', 'LOOTEX', 'STORE', 'CULT', 'ALKI', 'NKCLC', 'GGG', 'FALCONS', 'MTS', 'ERTHA', 'STON', 'POSI', 'BRISE', 'AVG', 'APP', 'LNR', 'LUFFY', 'CCD', 'NEXM', 'RADAR', 'SENATE', 'T', 'DOME', 'AQUA', 'LFW', 'GELATO', 'MR', 'CTR', 'SIDUS', 'GZONE', 'ALPINE', 'BSW', 'PHCR', 'SPELLFIRE', 'STARLY', 'PSTAKE', 'MGG', 'ARKN', 'VOW', 'APE', 'KUNCI', 'AIR', 'BCOIN', 'VOLT', 'RONIN', 'NBT', 'UMEE', 'NYM', 'TREEB', 'SD', 'GMT', 'STG', 'TRUEBIT', 'BONE', 'MULTI', 'HEROESC', 'ZBC', 'ALI', 'ASTO', 'UPO', 'GMX', 'MDAO', 'HBTC', 'GR', 'MMF', 'LMR', 'BRWL', 'QOM', 'MAGIC', 'INDI', 'QMALL', 'ORB', 'XCN', 'GST', 'TAKI', 'PHB', 'H2O', 'XRUN', 'STRM', 'GAL', 'USDD', 'EPX', 'REI', 'FITFI', 'EVMOS', 'MV', 'ORBR', 'ELU', 'SAO', '1EARTH', 'AOG', 'NRFB', 'PIXEL', 'KCT', 'ARCX', 'BRG', 'LUNA', 'OLAND', 'OP', 'JUNO', 'GNS', 'GEOJ', 'WAMPL', 'BREED', 'DFX', 'FORT', 'EKTA', 'SYNR', 'COL', 'GULF', 'IHC', 'MOVEZ', 'EAI', 'XDEN', 'THALES', 'EUL', 'HOP', 'KON', 'MOI', 'GDSC', 'LEAN', 'CHO', 'DCCT', 'JAM', 'OBX', 'GBD', 'LKC', 'WAGMIGAMES', 'SIN', 'IPV', 'XETA', 'LUXO', 'LBL', 'WE', 'LGX', 'FIU', 'SEOR', 'JUMBO', 'TAVA', 'BWO', 'FLZ', 'MVEDA', 'WLKN', 'GSTS', 'AZY', 'WOMBAT', 'HIBAYC', 'PKOIN', 'GBPT', 'FT', 'WBT', 'FOF', 'HIENS4', 'IJZ', 'DOGECUBE', 'QLINDO', 'HZM', 'DEFY', 'STC', 'ZPAY', 'CBETH', 'SQUIDGROW', 'ETHW', 'BLD', 'SWEAT', 'NEER', 'TAMA', 'SMR', 'AXL', 'TRIBL', 'MPLX', '00', 'APT', 'INERY', 'FER', 'BSX', 'INTR', 'NODL', 'PARALL', 'TEER', 'WAXL', 'JET', 'XPLA', 'POLYX', 'PROTO', 'OLE', 'RED', 'SML', 'RDNT', 'XSPECTAR', 'VICA', 'TCG2', 'RYOMA', '1SOL', 'AGLA', 'FDT', 'PUMLX', 'LING', 'HFT', 'ZZ', 'APEX', 'DLC', 'ELT', 'FAME', 'MMC', 'MNZ', 'OKG', 'SMCW', 'SKEB', 'EQ', 'MYTH', 'USDZ', 'PLY', 'TAP', 'MBASE', 'MNDE', 'BTCA', 'FOTA', 'ZEDTOKEN', 'SIGN', 'ASSA', 'ECOX', 'AVL', 'CCA', 'DOTR', 'IPX', 'NCDT', 'MF', 'METAV', 'PRTG', 'RB', 'RBD', 'LITHO', 'KAS', 'FBX', 'WWDOGE', 'FTN', 'HOOK', 'FACE', 'PEEL', 'BLUR', 'VERSE', 'AMKT', 'HDX', 'ING', 'BULL', 'IGU', 'ACS', 'ARB', 'ARTEQ', 'QUICK', 'NXRA', 'SUI', 'MDXH', 'SUDO', 'ID', 'PEPE', 'SEI', 'KARATE', 'DIMO', 'PANDOP', 'PRIME', 'WELL', 'LADYS', 'AI', 'VCORE', 'TOMI', 'MAV', 'CORE', 'WLD', 'MANTLE', 'ARKM', 'LSETH', 'PYUSD', 'FDUSD', 'ELS', 'ETH']

def fetch_data(symbol):
    data_list = cc.get_historical_price_day(symbol, 'usd')
    historical_data = [{'symbol': symbol, **data} for data in data_list]
    return historical_data

# Set the number of concurrent workers (adjust as needed)
num_workers = 7

historical_data = []

# Use concurrent.futures to fetch data concurrently
with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
    results = list(executor.map(fetch_data, symbols))

for result in results:
    historical_data.extend(result)

# Create a DataFrame from historical_data
df = pd.DataFrame(historical_data)

df = df[['time', 'symbol', 'close']]

# Load the scrape.csv data
scrape_data = pd.read_csv(r'C:\Users\alecj\python\Crypto\scrape.csv')

# Drop rows from df where 'symbol' in scrape_data has 'Market Cap' equal to 0
symbols_with_nonzero_market_cap = scrape_data[scrape_data['Market Cap'] != 0]['symbol']

# Drop rows from df where 'symbol' is not in scrape_data
df = df[df['symbol'].isin(scrape_data['symbol'])]

# Drop rows from df where 'symbol' in scrape_data has 'Market Cap' less than 50,000,000
scrape_data['Market Cap'] = pd.to_numeric(scrape_data['Market Cap'], errors='coerce')
symbols_with_market_cap_above_50M = scrape_data[scrape_data['Market Cap'] >= 50000000]['symbol']

#filters out those with mc less than 50m
df = df[df['symbol'].isin(symbols_with_market_cap_above_50M)]

df.to_csv(r"C:\Users\alecj\python\Crypto\historical_data.csv", index=False)

##### Append Base Data

import pandas as pd
import sqlite3

# Define the input and output file paths
input_csv = r'C:\Users\alecj\python\Crypto\historical_data.csv'
output_csv = r'C:\Users\alecj\python\Crypto\ml_data_clean.csv'

# Load the input data into a DataFrame
df = pd.read_csv(input_csv)

# Create an empty 'action' column
df['action'] = ''

# Create an SQLite database in memory
conn = sqlite3.connect(':memory:')

# Insert the DataFrame into the database
df.to_sql('crypto_data', conn, index=False, if_exists='replace')

# Define the logic
m = 1.1

# Apply the logic using SQL
sql_query = """
UPDATE crypto_data
SET action = CASE
    WHEN (SELECT close FROM crypto_data AS t2 WHERE t2.rowid = crypto_data.rowid + 1 AND t2.symbol = crypto_data.symbol) >= ? * close
        AND (SELECT action FROM crypto_data AS t3 WHERE t3.rowid = crypto_data.rowid - 1 AND t3.symbol = crypto_data.symbol) != 'buy'
    THEN 'buy'
    WHEN (SELECT action FROM crypto_data AS t4 WHERE t4.rowid = crypto_data.rowid - 1 AND t4.symbol = crypto_data.symbol) = 'buy'
    THEN 'sell'
    ELSE 'wait'
END
"""
conn.execute(sql_query, (m,))

# Fetch the updated data from the database
updated_df = pd.read_sql('SELECT * FROM crypto_data', conn)

# Close the database connection
conn.close()

# Save the updated DataFrame to the output CSV file
updated_df.to_csv(output_csv, index=False)

##### ML
# 
# import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

# Load the input data
input_csv = r'C:\Users\alecj\python\Crypto\ml_data_clean.csv'
output_csv = r'C:\Users\alecj\python\Crypto\ml_data_analysis.csv'
df = pd.read_csv(input_csv)

# Define the columns to be used for one-hot encoding and numerical features
categorical_features = ['symbol']
numeric_features = ['close']

# Update the categorical transformer with the new parameter name
categorical_transformer = Pipeline(steps=[
    ('onehot', OneHotEncoder(sparse_output=False))  # Set sparse_output to False
])

numeric_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('scaler', StandardScaler())
])

# Combine transformers using ColumnTransformer
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)
    ])

# Create the classifier model
clf = Pipeline(steps=[('preprocessor', preprocessor),
                      ('classifier', RandomForestClassifier())])

# Separate features and target variable
X = df[['symbol', 'close']]
y = df['action']

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Fit the model
clf.fit(X_train, y_train)

# Predict the action for the latest data
latest_data = df.groupby('symbol').last().reset_index()
y_pred = clf.predict(latest_data[['symbol', 'close']])

# Add the predicted action to the latest data
latest_data['predicted_action'] = y_pred

# Filter symbols with 'buy' in their predicted action
filtered_df = latest_data[latest_data['predicted_action'] == 'buy']

# Save the filtered DataFrame to a new CSV
filtered_df.to_csv(output_csv, index=False)

# Evaluate the prediction quality
y_pred = clf.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print(f'Accuracy: {accuracy:.2f}')

report = classification_report(y_test, y_pred)
print(f'Classification Report:\n{report}')

conf_matrix = confusion_matrix(y_test, y_pred)
print(f'Confusion Matrix:\n{conf_matrix}')