CREATE INDEX CONCURRENTLY idx_docs_content ON documents USING gin (content);
