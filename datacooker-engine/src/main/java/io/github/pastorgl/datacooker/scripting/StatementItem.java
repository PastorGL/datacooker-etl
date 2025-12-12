/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("unchecked")
public class StatementItem implements Serializable {
    final ProceduralStatement statement;
    final String[] control;
    final List<Expressions.ExprItem<?>>[] expression;
    final List<StatementItem> mainBranch;
    final List<StatementItem> elseBranch;

    public static class Builder {
        private final ProceduralStatement statement;
        private String[] control = null;
        private List<Expressions.ExprItem<?>>[] expression = null;
        private List<StatementItem> mainBranch = null;
        private List<StatementItem> elseBranch = null;

        public Builder(ProceduralStatement statement) {
            this.statement = statement;
        }

        public Builder control(String control) {
            this.control = new String[]{control};
            return this;
        }

        public Builder control(String[] control) {
            this.control = control;
            return this;
        }

        public Builder expression(List<Expressions.ExprItem<?>> expression) {
            this.expression = new List[]{expression};
            return this;
        }

        public Builder expression(List<Expressions.ExprItem<?>>[] expression) {
            this.expression = expression;
            return this;
        }

        public Builder mainBranch(List<StatementItem> mainBranch) {
            this.mainBranch = mainBranch;
            return this;
        }

        public Builder elseBranch(List<StatementItem> elseBranch) {
            this.elseBranch = elseBranch;
            return this;
        }

        public StatementItem build() {
            return new StatementItem(statement, control, expression, mainBranch, elseBranch);
        }
    }

    private StatementItem(ProceduralStatement statement, String[] control, List<Expressions.ExprItem<?>>[] expression, List<StatementItem> mainBranch, List<StatementItem> elseBranch) {
        this.statement = statement;
        this.control = control;
        this.expression = expression;
        this.mainBranch = mainBranch;
        this.elseBranch = elseBranch;
    }

    @Override
    public String toString() {
        return statement.name() + ((control != null) ? "$" + String.join(", $", control) : "");
    }
}
