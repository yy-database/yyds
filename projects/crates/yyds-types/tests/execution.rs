use yyds_types::{FileRef, Node, Program, Type, ValidatedProgram, Value, VectorMetric, VectorValue};

#[test]
fn yyds_consumes_the_pushed_yy_execution_model() {
    let program = ValidatedProgram::validate(Program {
        parameters: vec![Type::I64],
        inputs: vec![Type::I64],
        nodes: vec![
            Node::Parameter { index: 0, ty: Type::I64 },
            Node::Input { index: 0, ty: Type::I64 },
            Node::AddI64 { left: 0, right: 1 },
        ],
        output: 2,
        output_type: Type::I64,
    })
    .expect("program is valid");

    assert_eq!(program.evaluate(&[Value::I64(7)], &[Value::I64(5)]), Ok(Value::I64(12)));
}

#[test]
fn yyds_preserves_file_and_vector_execution_values() {
    let file = Value::File(FileRef {
        object_id: "asset-1".into(),
        generation: 3,
        byte_len: 4096,
    });
    assert_eq!(file.ty(), Type::File);

    let vector = VectorValue::new(vec![0.25, 0.5, 0.75], VectorMetric::Cosine)
        .expect("finite vector is valid");
    assert_eq!(Value::Vector(vector).ty(), Type::Vector {
        dimension: 3,
        metric: VectorMetric::Cosine,
    });
}
