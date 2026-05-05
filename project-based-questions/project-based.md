# Interview Feedback & Preparation Checklist

---

## From Today's Interview

1. **Data Volume Details**
   - What was the volume of data you handled? (GBs/TBs/PBs, number of records)
   - How much time did different processes/jobs take to complete?

2. **System Design Exploration**
   - What are the different methodologies to achieve the same outcome?
   - What are the constraints involved (budget, report latency, etc.)?
   - What are the pros and cons of each system design approach you explored?

3. **Pipeline Resilience & Observability**
   - How is your system fault tolerant?
   - How is schema evolution handled?
   - How is idempotency maintained in your pipeline?
   - How is a pipeline breakage/failure handled and recovered?
   - What is the alerting system in case of pipeline failure?

4. **Team Role & Contribution Clarity**
   - What was your role in the team?
   - Why were certain decisions/approaches taken?
   - Which specific parts of the system did you own and work on?
   - How did you handle challenges in your area?

5. **Data Warehouse Clarity**
   - Which data warehouse did you use to show data to business leaders?
   - Why was that particular warehouse chosen over alternatives?

6. **CDC, Optimization & Streaming Depth**
   - What is CDC (Change Data Capture) and how did you work with it?
   - How did you optimize your data and processes?
   - How was streaming data ingested and processed?
   - Why was a particular technology chosen and why were alternatives ruled out?

7. **Source Data Awareness**
   - What is the frequency of data arriving at the source?
   - What is the volume of data at the source level?
   - How did the source frequency/volume influence your pipeline design?

8. **Project Storytelling with Problem-Solution Depth**
   - Can you explain your project end-to-end in detail?
   - What were the real problems/challenges you faced?
   - How did you diagnose and resolve each problem?
   - What was the outcome/impact after resolution?

9. **Bottlenecks & Fault Tolerance Awareness**
   - What were the known bottlenecks in your pipeline?
   - How did you identify those bottlenecks?
   - What fault tolerance mechanisms were or could be implemented?

10. **Domain & Data Type Awareness**
    - What business domain(s) did you work in?
    - What types of data did you handle (structured, semi-structured, unstructured)?
    - What was the nature of the data (transactional, clickstream, IoT, master data)?
    - Were there any domain-specific compliance requirements (PII, GDPR)?

11. **Concurrency Management**
    - How is concurrency maintained in your data warehouse or tools?
    - How were concurrent loads/queries managed in your specific setup?

12. **Team Size & Composition**
    - What was the size of your team?
    - What roles did the team comprise of (DE, Architect, Analyst, DevOps, ML Engineer)?
    - How was the team structured and where did you fit in?

13. **Data Reprocessing on Job Failure**
    - How do you reprocess data if your jobs fail?
    - What strategies were used (backfill, checkpointing, Delta time travel)?
    - How did you avoid duplicate data during reprocessing?
    - Was reprocessing manual or automated?

14. **Business Impact of Your Project**
    - What business problem did your project solve?
    - What was the measurable impact (latency reduction, cost savings, accuracy %)?
    - Who benefited from the project?
    - How did it enable better decision-making or operational efficiency?

---

## Additional Gaps Identified

15. **Data Modeling Knowledge**
    - What data model was used (star schema, snowflake schema)?
    - Why was normalization or denormalization chosen?
    - How was the model designed for your specific use case?

16. **Data Quality Checks in Production**
    - What quality checks were built into your pipeline?
    - How were null checks, deduplication, and referential integrity handled?
    - How were data quality failures caught and managed?

17. **File Format Justification**
    - Why was a particular file format chosen (Parquet, ORC, Avro, JSON)?
    - What are the trade-offs between different file formats?

18. **Late-Arriving Data Handling**
    - How did your pipeline handle late-arriving or out-of-order data?
    - What watermarking or windowing strategies were applied in streaming?
    - How were corrections made to already-processed data?

19. **Exactly-Once vs At-Least-Once Processing**
    - Which processing guarantee did your pipeline provide?
    - How was it achieved technically?

20. **Small File Problem Awareness**
    - How did small files impact performance in your distributed system?
    - What strategies were used to address the small file problem?

21. **Data Governance & Security**
    - What access controls, data masking, or audit logging were applied?
    - Was any lineage tracking or governance framework used?

22. **Communicating Technical Concepts to Non-Technical Stakeholders**
    - Can you give an example of explaining a pipeline issue to business leaders?
    - How did you simplify technical decisions for non-technical audiences?

23. **Scaling Strategy**
    - How did your system scale as data volumes grew?
    - Was horizontal or vertical scaling used?
    - How was auto-scaling configured?

24. **Behavioral Questions — STAR Method**
    - Prepare structured answers using: Situation → Task → Action → Result
    - Have 3–5 stories ready: a debugging win, cross-team collaboration, a mistake learned from, a disagreement resolved, a time you went beyond your role.
