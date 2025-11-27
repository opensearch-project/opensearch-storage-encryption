# opensearch-storage-encryption

An Opensearch plugin for supporting "fast" On fly Index-Level-Encryption. Security with high Performance is of highest 
priority. 

## Plugin Modes

The crypto directory plugin can operate in two modes:

### 1. **Disabled Mode (Default)**
- Plugin is loaded but all encryption functionality is inactive
- No performance overhead from encryption operations
- This is the **default state** - no configuration needed

### 2. **Enabled Mode**
- Plugin performs encryption/decryption operations
- All crypto directory features are active
- Encrypted indices can be created and accessed
- To enable, add to `opensearch.yml`:
  ```yaml
  plugins.crypto.enabled: true
  ```

**⚠️ Important Notes:**
- The enabled setting requires node restart to change
- Plugin is **disabled by default** - you must explicitly enable it for encryption
- Existing encrypted indices become inaccessible when plugin is disabled
- Setting should be consistent across all cluster nodes for best results
- Cannot create new encrypted indices when disabled (`cryptofs` store type unavailable)

# Architecture

```


Node 

┌─────────────┐                 ┌─────────────────────────────────────────────┐                 
│   Tenant A  │                 │             OpenSearch App                  │                                      
│ (plain text)│ ────plain────→  │                                             │                                            
└─────────────┘                 │  ┌─────────────────┐      plain text        │  ┌─────────────┐│                            
                                │  │ HybridDirectory │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─→ │  Tenant A   ││
                                │  │                 │                         │  │   index     ││
                                │  └─────────────────┘                         │  │   shards    ││
                                │                                              │  └─────────────┘│
┌─────────────┐                 │                                              │  ┌─────────────┐│
│   Tenant B  │                 │  ┌─────────────────┐      cipher text        │  │  Tenant B   ││
│ (encrypted) │ ────plain────→  │  │ CryptoDirectory │ ═ ═ ═ ═ ═ ═ ═ ═ ═ ═ ═ ═ ═→ │   index     ││
└─────────────┘                 │  │      🔑         │                         │  │   shards    ││
                                │  └─────────────────┘                         │  │     🔑      ││
                                │           │                                  │  └─────────────┘│
                                └───────────┼──────────────────────────────────┘                 
                                            ▼ generate or decrypt                               
                                             data key                                           
                                ┌─────────────────────────┐                                     
                                │    Tenant B KMS (🔐)     │                                     
                                │   Key Management Service │                                     
                                └─────────────────────────┘                                     
                                                                                               

```



## Key Components

We implement a new Lucene Directory (NioFS and MMAP) that will encrypt or decrypt shard data on the fly. We can use existing settings.index.store.type configuration to enable encryption when we create an index. Currently we only support KMS for key management but it can be extended in future

For example:

```
 "index_settings": {
    "index.store.type": "cryptofs",
    "index.store.crypto.kms.type": "aws-kms"
}

```

## Key announcement  

29/7/2025: The plugin development is still in progress and is expected to land fully in Opensearch 3.3 release.
