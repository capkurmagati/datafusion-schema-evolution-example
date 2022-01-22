use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::datasource::object_store::local::LocalFileSystem;
use datafusion::datasource::{PartitionedFile, TableProvider};
use datafusion::error::Result;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::file_format::{ParquetExec, PhysicalPlanConfig};
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use std::any::Any;
use std::sync::Arc;

struct MyTable {
    pub path: String,
    pub schema: SchemaRef,
}

impl MyTable {
    pub fn new(path: &str, schema: Schema) -> Self {
        MyTable {
            path: path.to_string(),
            schema: Arc::new(schema),
        }
    }
}

#[async_trait]
impl TableProvider for MyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let files = get_file_from_path(&self.path);
        let config = PhysicalPlanConfig {
            object_store: Arc::new(LocalFileSystem {}),
            file_groups: vec![files],
            file_schema: self.schema(),
            statistics: Statistics::default(),
            projection: projection.clone(),
            batch_size,
            limit,
            table_partition_cols: vec![],
        };
        Ok(Arc::new(ParquetExec::new(config, None)))
    }
}

pub fn get_file_from_path(_path: &str) -> Vec<PartitionedFile> {
    vec![PartitionedFile::new("./data/v1.parquet".to_string(), 623), PartitionedFile::new("./data/v2.parquet".to_string(), 991)]
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use crate::MyTable;
    use arrow::datatypes::{Schema, Field, DataType};
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::execution::context::ExecutionContext;
    use datafusion::physical_plan::collect;
    use std::sync::Arc;

    #[tokio::test]
    async fn table_scan() {
        let field_a = Field::new("col_1", DataType::Int32, false);
        let field_b = Field::new("col_2", DataType::Int32, false);
        let schema = Schema::new(vec![field_a, field_b]);
        let table = MyTable::new("./data", schema);
        let mut ctx = ExecutionContext::new();
        ctx.register_table("a", Arc::new(table)).unwrap();
        // project the common column
        let sql = "select col_1 from a order by 1";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+-------+",
            "| col_1 |",
            "+-------+",
            "| 1     |",
            "| 1     |",
            "| 2     |",
            "| 2     |",
            "| 3     |",
            "| 3     |",
            "+-------+",
        ];
        assert_batches_sorted_eq!(expected, &actual);

        // project all collumns
        let sql = "select * from a order by 1";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+-------+-------+",
            "| col_1 | col_2 |",
            "+-------+-------+",
            "| 1     | NULL  |",
            "| 1     | 1     |",
            "| 2     | NULL  |",
            "| 2     | 2     |",
            "| 3     | NULL  |",
            "| 3     | 3     |",
            "+-------+-------+",
        ];
        assert_batches_sorted_eq!(expected, &actual);
    }

    // Copy from arrow-datafusion/datafusion/tests/sql.rs
    async fn execute_to_batches(ctx: &mut ExecutionContext, sql: &str) -> Vec<RecordBatch> {
        let msg = format!("Creating logical plan for '{}'", sql);
        let plan = ctx.create_logical_plan(sql).expect(&msg);
        let logical_schema = plan.schema();

        let msg = format!("Optimizing logical plan for '{}': {:?}", sql, plan);
        let plan = ctx.optimize(&plan).expect(&msg);
        let optimized_logical_schema = plan.schema();

        let msg = format!("Creating physical plan for '{}': {:?}", sql, plan);
        let plan = ctx.create_physical_plan(&plan).await.expect(&msg);

        let msg = format!("Executing physical plan for '{}': {:?}", sql, plan);
        let results = collect(plan).await.expect(&msg);

        assert_eq!(logical_schema.as_ref(), optimized_logical_schema.as_ref());
        results
    }
}
