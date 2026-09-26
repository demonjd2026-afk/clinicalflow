# ClinicalFlow — Project Structure

```
clinicalflow/                                  ← git repo root
│
├── README.md                                  ✅ done
├── SETUP.md                                   ✅ done
├── clinicalflow-qa.md                         ✅ done
├── databricks.yml                             ⬜ pending
├── variables.yml                              ⬜ pending
│
├── resources/
│   ├── pipelines/
│   │   └── lakeflow_pipeline.yml              ⬜ pending
│   ├── jobs/
│   │   ├── ingest_job.yml                     ⬜ pending
│   │   ├── streaming_job.yml                  ⬜ pending
│   │   ├── maintenance_job.yml                ⬜ pending
│   │   └── genai_job.yml                      ⬜ pending
│   └── clusters/
│       └── job_cluster_policy.yml             ⬜ pending
│
├── src/
│   ├── notebooks/
│   │   ├── 01_unity_catalog_setup.py          ✅ done
│   │   ├── 02_data_generation.py              ✅ done
│   │   ├── 03_delta_time_travel.py            ⬜ pending
│   │   ├── 04_optimization.py                 ⬜ pending
│   │   └── 05_ai_query_demo.py                ⬜ pending
│   │
│   ├── pipelines/
│   │   ├── pipeline_batch.py                  ⬜ pending
│   │   ├── pipeline_streaming.py              ⬜ pending
│   │   ├── scd_transforms.py                  ⬜ pending
│   │   ├── gold_aggregations.py               ⬜ pending
│   │   └── expectations.py                    ⬜ pending
│   │
│   └── genai/
│       ├── fmapi_enrichment.py                ⬜ pending
│       ├── vector_search_setup.py             ⬜ pending
│       └── rag_query.py                       ⬜ pending
│
├── data/
│   └── icd10_codes.csv                        ⬜ pending
│
├── tests/
│   ├── test_dq_expectations.py                ⬜ pending
│   ├── test_scd_logic.py                      ⬜ pending
│   └── test_merge_dedup.py                    ⬜ pending
│
├── .github/
│   └── workflows/
│       └── deploy.yml                         ⬜ pending
│
└── .gitignore                                 ⬜ pending
```
