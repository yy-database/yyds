use yyds_types::{Node, Program, Type, Udf, UdfEffect, UdfPlacement, Value};

#[test]
fn yyds_evaluates_a_manually_constructed_distributed_udf() {
    let udf = Udf {
        id: "score.add".into(),
        version: 1,
        deterministic: true,
        effect: UdfEffect::Read,
        placement: UdfPlacement::Shard,
        program: Program {
            parameters: vec![],
            inputs: vec![Type::I64, Type::I64],
            nodes: vec![
                Node::Input { index: 0, ty: Type::I64 },
                Node::Input { index: 1, ty: Type::I64 },
                Node::AddI64 { left: 0, right: 1 },
            ],
            output: 2,
            output_type: Type::I64,
        },
    }
    .validate()
    .expect("distributed UDF program is valid");

    assert_eq!(udf.id(), "score.add");
    assert_eq!(udf.evaluate(&[], &[Value::I64(4), Value::I64(6)]), Ok(Value::I64(10)));
}
