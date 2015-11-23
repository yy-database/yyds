use crate::{SqlElementType, SqlLanguage, ast::*, builder::SqlBuilder, lexer::token_type::SqlTokenType};
use oak_core::{OakError, Range, RedNode, RedTree, SourceText};
use std::sync::Arc;

impl<'config> SqlBuilder<'config> {
    /// Builds a typed expression from an untyped expression node.
    pub(crate) fn build_expression<'a>(&self, node: RedNode<'a, SqlLanguage>, source: &SourceText) -> Result<Expression, OakError> {
        self.build_expression_with_precedence(node, source, 0)
    }

    /// Internal recursive method to build expressions while respecting operator precedence.
    ///
    /// It uses a "precedence climbing" algorithm to correctly structure binary operations
    /// when the input CST is flat or partially structured.
    fn build_expression_with_precedence<'a>(&self, node: RedNode<'a, SqlLanguage>, source: &SourceText, min_precedence: u8) -> Result<Expression, OakError> {
        let children = node.children().collect::<Vec<_>>();

        // Handle parentheses — only unwrap when there is a single inner expression.
        // Multiple comma-separated expressions (e.g. IN lists `(1,2,3)`) must fall
        // through to build_primary_expression which detects LeftParen + expr_children > 1
        // and routes to build_vector.
        if children.len() >= 3 {
            if let (RedTree::Leaf(l), RedTree::Leaf(r)) = (&children[0], &children[children.len() - 1]) {
                if l.kind == SqlTokenType::LeftParen && r.kind == SqlTokenType::RightParen {
                    let expr_count = children[1..children.len() - 1]
                        .iter()
                        .filter(|c| matches!(c, RedTree::Node(n) if n.green.kind == SqlElementType::Expression))
                        .count();
                    if expr_count == 1 {
                        for child in &children[1..children.len() - 1] {
                            if let RedTree::Node(n) = child {
                                if n.green.kind == SqlElementType::Expression {
                                    return self.build_expression_with_precedence(n.clone(), source, 0);
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut left = self.build_primary_expression(node.clone(), source)?;

        // Precedence climbing
        // This is tricky because the CST might already be structured or flat.
        // If it's flat, we iterate through children.
        // If it's already structured (e.g. nested Expression nodes), we might need to handle it differently.

        let mut i = 0;
        while i < children.len() {
            let child = &children[i];
            if let RedTree::Leaf(op_token) = child {
                let kind = op_token.kind;

                // === BETWEEN ===
                if kind == SqlTokenType::Between {
                    let precedence = 2; // 与 parser precedence 同步
                    if precedence >= min_precedence {
                        i += 1;
                        while i < children.len() && matches!(children[i], RedTree::Leaf(l) if l.kind == SqlTokenType::Whitespace) { i += 1; }
                        if let Some(RedTree::Node(n)) = children.get(i) {
                            let right_expr = self.build_expression_with_precedence(n.clone(), source, 0)?;
                            match right_expr {
                                Expression::Binary { left: low, op: BinaryOperator::And, right: high, span: _ } => {
                                    let span = Range { start: left.span().start, end: high.span().end };
                                    left = Expression::Between { expr: Box::new(left), low, high, negated: false, span };
                                    continue;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                // === IS NULL / IS NOT NULL ===
                if kind == SqlTokenType::Is {
                    let precedence = 2;
                    if precedence >= min_precedence {
                        i += 1;
                        while i < children.len() && matches!(children[i], RedTree::Leaf(l) if l.kind == SqlTokenType::Whitespace) { i += 1; }
                        if let Some(RedTree::Node(n)) = children.get(i) {
                            let right_expr = self.build_expression_with_precedence(n.clone(), source, 0)?;
                            // BinaryOperator 没有 Is — 临时用 BinaryOperator::Equal 表示"等于 Null"
                            let span = Range { start: left.span().start, end: right_expr.span().end };
                            left = Expression::Binary { left: Box::new(left), op: BinaryOperator::Equal, right: Box::new(right_expr), span };
                            continue;
                        }
                    }
                }

                // === IN ===
                if kind == SqlTokenType::In {
                    let precedence = 2;
                    if precedence >= min_precedence {
                        i += 1;
                        while i < children.len() && matches!(children[i], RedTree::Leaf(l) if l.kind == SqlTokenType::Whitespace) { i += 1; }
                        if let Some(RedTree::Node(n)) = children.get(i) {
                            let right_expr = self.build_expression_with_precedence(n.clone(), source, 0)?;
                            match &right_expr {
                                Expression::Vector { elements, .. } => {
                                    let span = Range { start: left.span().start, end: right_expr.span().end };
                                    left = Expression::InList { expr: Box::new(left), list: elements.clone(), negated: false, span };
                                    continue;
                                }
                                _ => {
                                    // Pratt 对逗号列表处理有限 — 跳过，留给 execute 层或后续修复
                                }
                            }
                        }
                    }
                }
                else if let Some(op) = self.map_binary_op(kind) {
                    let precedence = self.get_precedence(kind);
                    if precedence >= min_precedence {
                        i += 1;
                        // Find the next operand
                        let mut next_operand = None;
                        while i < children.len() {
                            match &children[i] {
                                RedTree::Node(n) => {
                                    if n.green.kind == SqlElementType::Expression || n.green.kind == SqlElementType::Identifier {
                                        next_operand = Some(self.build_expression_with_precedence(n.clone(), source, precedence + 1)?);
                                        break;
                                    }
                                }
                                RedTree::Leaf(t) => {
                                    if matches!(t.kind, SqlTokenType::NumberLiteral | SqlTokenType::StringLiteral | SqlTokenType::True | SqlTokenType::False | SqlTokenType::Null) {
                                        next_operand = Some(self.build_primary_expression_from_leaf(t, source)?);
                                        break;
                                    }
                                }
                            }
                            i += 1;
                        }

                        if let Some(right) = next_operand {
                            let span = Range { start: left.span().start, end: right.span().end };
                            left = Expression::Binary { left: Box::new(left), op, right: Box::new(right), span };
                            continue;
                        }
                    }
                }
            }
            i += 1;
        }

        Ok(left)
    }

    fn build_primary_expression<'a>(&self, node: RedNode<'a, SqlLanguage>, source: &SourceText) -> Result<Expression, OakError> {
        // Dispatch on node kind FIRST — FunctionCall nodes may have Identifier children
        // that would otherwise be picked up first by the child loop below.
        if node.green.kind == SqlElementType::FunctionCall {
            return self.build_function_call(node, source);
        }

        // Count expression children — LeftParen with multiple (comma-separated) children
        // is a Vector; a single Expression child is a plain grouped expression.
        let expr_children = node.children().filter(|c| matches!(c, RedTree::Node(n) if n.green.kind == SqlElementType::Expression)).count();
        for child in node.children() {
            match child {
                RedTree::Leaf(t) => {
                    if t.kind == SqlTokenType::LeftBracket {
                        return self.build_vector(node.clone(), source);
                    }
                    if t.kind == SqlTokenType::LeftParen && expr_children > 1 {
                        return self.build_vector(node.clone(), source);
                    }
                    if let Ok(expr) = self.build_primary_expression_from_leaf(&t, source) {
                        return Ok(expr);
                    }
                }
                RedTree::Node(n) => {
                    if n.green.kind == SqlElementType::Expression {
                        return self.build_expression(n, source);
                    }
                    else if n.green.kind == SqlElementType::Identifier {
                        return Ok(Expression::Identifier(self.build_identifier(n, source)?));
                    }
                    else if n.green.kind == SqlElementType::FunctionCall {
                        return self.build_function_call(n, source);
                    }
                    else if n.green.kind == SqlElementType::Subquery {
                        return self.build_subquery(n, source);
                    }
                }
            }
        }
        Err(OakError::custom_error("Failed to build primary expression"))
    }

    fn build_primary_expression_from_leaf(&self, t: &oak_core::RedLeaf<SqlLanguage>, source: &SourceText) -> Result<Expression, OakError> {
        match t.kind {
            SqlTokenType::NumberLiteral | SqlTokenType::FloatLiteral => {
                let text = self.get_text(t.span.clone(), source);
                let trimmed = text.trim();
                let value = if trimmed.len() == text.len() { text } else { Arc::from(trimmed) };
                Ok(Expression::Literal(Literal::Number(value, t.span.clone())))
            }
            SqlTokenType::StringLiteral => {
                let text = self.get_text(t.span.clone(), source);
                let trimmed = text.trim();
                let content = if (trimmed.starts_with('\'') && trimmed.ends_with('\'')) || (trimmed.starts_with('"') && trimmed.ends_with('"')) { if trimmed.len() >= 2 { &trimmed[1..trimmed.len() - 1] } else { "" } } else { trimmed };
                Ok(Expression::Literal(Literal::String(Arc::from(content), t.span.clone())))
            }
            SqlTokenType::True | SqlTokenType::False => Ok(Expression::Literal(Literal::Boolean(t.kind == SqlTokenType::True, t.span.clone()))),
            SqlTokenType::Null => Ok(Expression::Literal(Literal::Null(t.span.clone()))),
            // Identifier leaf — 从 build_primary_expression 里 Leaf(Identifier_) 子节点进入，
            // 构造 Expression::Identifier (与 build_identifier 同逻辑)。
            SqlTokenType::Identifier_ => {
                Ok(Expression::Identifier(Identifier { name: self.get_text(t.span.clone(), source), span: t.span.clone() }))
            }
            _ => Err(OakError::custom_error("Not a primary expression leaf")),
        }
    }

    fn build_literal<'a>(&self, node: RedNode<'a, SqlLanguage>, source: &SourceText) -> Result<Literal, OakError> {
        for child in node.children() {
            if let RedTree::Leaf(t) = child {
                match t.kind {
                    SqlTokenType::NumberLiteral => return Ok(Literal::Number(self.get_text(t.span.clone(), source), t.span.clone())),
                    SqlTokenType::StringLiteral => {
                        let text = self.get_text(t.span.clone(), source);
                        let content = text.trim_matches('\'');
                        return Ok(Literal::String(Arc::from(content), t.span.clone()));
                    }
                    SqlTokenType::True => return Ok(Literal::Boolean(true, t.span.clone())),
                    SqlTokenType::False => return Ok(Literal::Boolean(false, t.span.clone())),
                    SqlTokenType::Null => return Ok(Literal::Null(t.span.clone())),
                    _ => {}
                }
            }
        }
        Err(OakError::custom_error("Missing literal content"))
    }

    /// Maps a token kind to its operator precedence level.
    fn get_precedence(&self, kind: SqlTokenType) -> u8 {
        match kind {
            SqlTokenType::Or => 1,
            SqlTokenType::Between | SqlTokenType::In | SqlTokenType::Is => 2,
            SqlTokenType::And => 3,
            SqlTokenType::Equal | SqlTokenType::NotEqual | SqlTokenType::Less | SqlTokenType::Greater | SqlTokenType::LessEqual | SqlTokenType::GreaterEqual | SqlTokenType::Like => 4,
            SqlTokenType::Concat => 5,
            SqlTokenType::Plus | SqlTokenType::Minus => 10,
            SqlTokenType::Star | SqlTokenType::Slash => 11,
            _ => 0,
        }
    }

    fn build_function_call<'a>(&self, node: RedNode<'a, SqlLanguage>, source: &SourceText) -> Result<Expression, OakError> {
        let mut name = None;
        let mut args = Vec::new();

        for child in node.children() {
            match child {
                RedTree::Node(n) => {
                    if n.green.kind == SqlElementType::Identifier {
                        // First Identifier child is the function name. Subsequent ones
                        // (from bare identifier args like MAX(id)) are args → Expression::Identifier.
                        if name.is_none() {
                            name = Some(self.build_identifier(n, source)?);
                        } else {
                            args.push(Expression::Identifier(self.build_identifier(n, source)?));
                        }
                    }
                    else if n.green.kind == SqlElementType::Expression {
                        args.push(self.build_expression(n, source)?);
                    }
                }
                _ => {}
            }
        }

        Ok(Expression::FunctionCall { name: name.ok_or_else(|| OakError::custom_error("Missing function name"))?, args, span: node.span() })
    }

    fn build_subquery<'a>(&self, node: RedNode<'a, SqlLanguage>, source: &SourceText) -> Result<Expression, OakError> {
        for child in node.children() {
            if let RedTree::Node(n) = child {
                if n.green.kind == SqlElementType::SelectStatement {
                    return Ok(Expression::Subquery { query: Box::new(self.build_select_statement(n, source)?), span: node.span() });
                }
            }
        }
        Err(OakError::custom_error("Missing select statement in subquery"))
    }

    fn build_vector<'a>(&self, node: RedNode<'a, SqlLanguage>, source: &SourceText) -> Result<Expression, OakError> {
        let mut elements = Vec::new();
        for child in node.children() {
            if let RedTree::Node(n) = child {
                if n.green.kind == SqlElementType::Expression {
                    elements.push(self.build_expression(n, source)?);
                }
            }
        }
        Ok(Expression::Vector { elements, span: node.span() })
    }
}
