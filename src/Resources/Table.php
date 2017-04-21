<?php

namespace DreamFactory\Core\Firebird\Resources;

use DreamFactory\Core\Database\Enums\DbFunctionUses;
use DreamFactory\Core\Enums\DbLogicalOperators;
use DreamFactory\Core\Enums\DbComparisonOperators;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Firebird\Database\Query\Grammars\Grammar;

class Table extends \DreamFactory\Core\SqlDb\Resources\Table
{
    /** {@inheritdoc} */
    protected function parseFilterString($filter, array &$out_params, $fields_info, array $in_params = [])
    {
        if (empty($filter)) {
            return null;
        }

        $filter = trim($filter);
        // todo use smarter regex
        // handle logical operators first
        $logicalOperators = DbLogicalOperators::getDefinedConstants();
        foreach ($logicalOperators as $logicalOp) {
            if (DbLogicalOperators::NOT_STR === $logicalOp) {
                // NOT(a = 1)  or NOT (a = 1)format
                if ((0 === stripos($filter, $logicalOp . ' (')) || (0 === stripos($filter, $logicalOp . '('))) {
                    $parts = trim(substr($filter, 3));
                    $parts = $this->parseFilterString($parts, $out_params, $fields_info, $in_params);

                    return static::localizeOperator($logicalOp) . $parts;
                }
            } else {
                // (a = 1) AND (b = 2) format or (a = 1)AND(b = 2) format
                $filter = str_ireplace(')' . $logicalOp . '(', ') ' . $logicalOp . ' (', $filter);
                $paddedOp = ') ' . $logicalOp . ' (';
                if (false !== $pos = stripos($filter, $paddedOp)) {
                    $left = trim(substr($filter, 0, $pos)) . ')'; // add back right )
                    $right = '(' . trim(substr($filter, $pos + strlen($paddedOp))); // adding back left (
                    $left = $this->parseFilterString($left, $out_params, $fields_info, $in_params);
                    $right = $this->parseFilterString($right, $out_params, $fields_info, $in_params);

                    return $left . ' ' . static::localizeOperator($logicalOp) . ' ' . $right;
                }
            }
        }

        $wrap = false;
        if ((0 === strpos($filter, '(')) && ((strlen($filter) - 1) === strrpos($filter, ')'))) {
            // remove unnecessary wrapping ()
            $filter = substr($filter, 1, -1);
            $wrap = true;
        }

        // Some scenarios leave extra parens dangling
        $pure = trim($filter, '()');
        $pieces = explode($pure, $filter);
        $leftParen = (!empty($pieces[0]) ? $pieces[0] : null);
        $rightParen = (!empty($pieces[1]) ? $pieces[1] : null);
        $filter = $pure;

        // the rest should be comparison operators
        // Note: order matters here!
        $sqlOperators = DbComparisonOperators::getParsingOrder();
        foreach ($sqlOperators as $sqlOp) {
            $paddedOp = static::padOperator($sqlOp);
            if (false !== $pos = stripos($filter, $paddedOp)) {
                $field = trim(substr($filter, 0, $pos));
                $negate = false;
                if (false !== strpos($field, ' ')) {
                    $parts = explode(' ', $field);
                    $partsCount = count($parts);
                    if (($partsCount > 1) &&
                        (0 === strcasecmp($parts[$partsCount - 1], trim(DbLogicalOperators::NOT_STR)))
                    ) {
                        // negation on left side of operator
                        array_pop($parts);
                        $field = implode(' ', $parts);
                        $negate = true;
                    }
                }
                /** @type ColumnSchema $info */
                if (null === $info = array_get($fields_info, strtolower($field))) {
                    // This could be SQL injection attempt or bad field
                    throw new BadRequestException("Invalid or unparsable field in filter request: '$field'");
                }

                // make sure we haven't chopped off right side too much
                $value = trim(substr($filter, $pos + strlen($paddedOp)));
                if ((0 !== strpos($value, "'")) &&
                    (0 !== $lpc = substr_count($value, '(')) &&
                    ($lpc !== $rpc = substr_count($value, ')'))
                ) {
                    // add back to value from right
                    $parenPad = str_repeat(')', $lpc - $rpc);
                    $value .= $parenPad;
                    $rightParen = preg_replace('/\)/', '', $rightParen, $lpc - $rpc);
                }
                if (DbComparisonOperators::requiresValueList($sqlOp)) {
                    if ((0 === strpos($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
                        // remove wrapping ()
                        $value = substr($value, 1, -1);
                        $parsed = [];
                        foreach (explode(',', $value) as $each) {
                            $parsed[] = $this->parseFilterValue(trim($each), $info, $out_params, $in_params);
                        }
                        $value = '(' . implode(',', $parsed) . ')';
                    } else {
                        throw new BadRequestException('Filter value lists must be wrapped in parentheses.');
                    }
                } elseif (DbComparisonOperators::requiresNoValue($sqlOp)) {
                    $value = null;
                } else {
                    static::modifyValueByOperator($sqlOp, $value);
                    $value = $this->parseFilterValue($value, $info, $out_params, $in_params);
                }

                $sqlOp = static::localizeOperator($sqlOp);
                if ($negate) {
                    $sqlOp = DbLogicalOperators::NOT_STR . ' ' . $sqlOp;
                }

                if ($function = $info->getDbFunction(DbFunctionUses::FILTER)) {
                    $out = $this->dbConn->raw($function);
                } else {
                    if (Grammar::isKeyword($info->name)) {
                        $out = $info->quotedName;
                    } else {
                        $out = $info->name;
                    }
                }
                $out .= " $sqlOp";

                $out .= (isset($value) ? " $value" : null);
                if ($leftParen) {
                    $out = $leftParen . $out;
                }
                if ($rightParen) {
                    $out .= $rightParen;
                }

                return ($wrap ? '(' . $out . ')' : $out);
            }
        }

        // This could be SQL injection attempt or unsupported filter arrangement
        throw new BadRequestException('Invalid or unparsable filter request.');
    }
}