use yyds_types::{Node, Program, Type, ValidatedProgram, Value};

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
