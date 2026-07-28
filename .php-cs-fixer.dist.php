<?php

declare(strict_types=1);

use PhpCsFixer\Config;
use PhpCsFixer\Finder;

$finder = Finder::create()
    ->in([__DIR__])
    ->name('*.php')
    ->exclude('vendor')
    ->ignoreDotFiles(true)
    ->ignoreVCS(true)
;

$rules = [
    // Alias
    'array_push' => true,
    'ereg_to_preg' => true,
    'modernize_strpos' => true,
    'no_alias_functions' => [
        'sets' => ['@all'],
    ],
    'no_alias_language_construct_call' => true,
    'random_api_migration' => true,
    'set_type_to_cast' => true,

    // Array Notation
    'array_syntax' => [
        'syntax' => 'short',
    ],
    'no_multiline_whitespace_around_double_arrow' => true,
    'no_trailing_comma_in_singleline_array' => true,
    'normalize_index_brace' => true,
    'return_to_yield_from' => true,
    'trim_array_spaces' => true,
    'whitespace_after_comma_in_array' => [
        'ensure_single_space' => true,
    ],

    // Attribute Notation
    'attribute_empty_parentheses' => true,
    'ordered_attributes' => true,

    // Basic
    'braces_position' => true,
    'encoding' => true,
    'no_multiple_statements_per_line' => true,
    'octal_notation' => true,
    'single_line_empty_body' => false,
    'psr_autoloading' => true,

    // Casing
    'class_reference_name_casing' => true,
    'constant_case' => true,
    'integer_literal_case' => true,
    'lowercase_keywords' => true,
    'lowercase_static_reference' => true,
    'magic_constant_casing' => true,
    'magic_method_casing' => true,
    'native_function_casing' => true,
    'native_type_declaration_casing' => true,

    // Cast Notation
    'cast_spaces' => ['space' => 'single'],
    'lowercase_cast' => true,
    'modernize_types_casting' => true,
    'no_short_bool_cast' => true,
    'short_scalar_cast' => true,

    // Class Notation
    'class_attributes_separation' => false,
    'class_definition' => [
        'single_item_single_line' => true,
        'multi_line_extends_each_single_line' => true,
        'space_before_parenthesis' => true,
    ],
    'no_blank_lines_after_class_opening' => true,
    'phpdoc_readonly_class_comment_to_keyword' => true,
    'self_accessor' => true,
    'self_static_accessor' => true,
    'single_class_element_per_statement' => true,
    'visibility_required' => true,

    // Class Usage
    'date_time_immutable' => true,
    'no_unused_imports' => true,

    // Comment
    'comment_to_phpdoc' => true,
    'multiline_comment_opening_closing' => true,
    'no_empty_comment' => false,
    'no_trailing_whitespace_in_comment' => true,
    'single_line_comment_spacing' => true,
    'single_line_comment_style' => true,

    // Constant Notation
    'native_constant_invocation' => true,

    // Control Structure
    'control_structure_braces' => true,
    'control_structure_continuation_position' => true,
    'elseif' => true,
    'empty_loop_body' => true,
    'empty_loop_condition' => ['style' => 'for'],
    'include' => true,
    'no_alternative_syntax' => true,
    'no_break_comment' => true,
    'no_trailing_comma_in_list_call' => true,
    'no_unneeded_control_parentheses' => true,
    'no_useless_else' => true,
    'simplified_if_return' => false,
    'switch_case_space' => true,
    'trailing_comma_in_multiline' => [
        'elements' => ['arguments', 'arrays', 'match', 'parameters'],
    ],
    'yoda_style' => true,

    // Function Notation
    'combine_nested_dirname' => true,
    'date_time_create_from_format_call' => true,
    'fopen_flag_order' => true,
    'function_declaration' => true,
    'function_typehint_space' => true,
    'implode_call' => true,
    'lambda_not_used_import' => false,
    'method_argument_space' => true,
    'native_function_invocation' => [
        'scope' => 'all',
        'strict' => true,
        'include' => ['@all'],
        // 'include' => ['@compiler_optimized'],
    ],
    'no_spaces_after_function_name' => true,
    'no_useless_sprintf' => true,
    'regular_callable_call' => true,
    'return_type_declaration' => true,
    'single_line_throw' => false,
    'static_lambda' => true,
    'use_arrow_functions' => true,
    'void_return' => true,

    // Import
    'fully_qualified_strict_types' => true,
    'global_namespace_import' => [
        'import_classes' => false,
        'import_constants' => false,
        'import_functions' => false,
    ],
    'no_unneeded_import_alias' => true,
    'single_line_after_imports' => true,

    // Language Construct
    'class_keyword' => true,
    'combine_consecutive_issets' => true,
    'declare_equal_normalize' => true,
    'declare_parentheses' => true,
    'dir_constant' => true,
    'explicit_indirect_variable' => true,
    'function_to_constant' => true,
    'get_class_to_class_keyword' => true,
    'is_null' => true,
    'nullable_type_declaration' => true,
    'single_space_after_construct' => true,
    'single_space_around_construct' => true,

    // List Notation
    'list_syntax' => true,

    // Namespace Notation
    'blank_line_after_namespace' => true,
    'blank_lines_before_namespace' => true,
    'clean_namespace' => true,

    // Naming
    'no_homoglyph_names' => true,

    // Operator
    'assign_null_coalescing_to_coalesce_equal' => true,
    'binary_operator_spaces' => true,
    'concat_space' => ['spacing' => 'one'],
    'logical_operators' => true,
    'long_to_shorthand_operator' => true,
    'new_with_parentheses' => [
        'anonymous_class' => false,
        'named_class' => false,
    ],
    'no_space_around_double_colon' => true,
    'no_useless_concat_operator' => true,
    'not_operator_with_successor_space' => true,
    'object_operator_without_whitespace' => true,
    'operator_linebreak' => [
        'only_booleans' => false,
        'position' => 'beginning',
    ],
    'standardize_increment' => true,
    'standardize_not_equals' => true,
    'ternary_operator_spaces' => true,
    'ternary_to_elvis_operator' => true,
    'ternary_to_null_coalescing' => true,
    'unary_operator_spaces' => true,

    // PHP Tag
    'blank_line_after_opening_tag' => true,
    'full_opening_tag' => true,
    'no_closing_tag' => true,

    // PHPDoc
    'align_multiline_comment' => true,
    'no_blank_lines_after_phpdoc' => true,
    'no_empty_phpdoc' => true,
    'phpdoc_align' => true,
    'phpdoc_annotation_without_dot' => true,
    'phpdoc_indent' => true,
    'phpdoc_line_span' => true,
    'phpdoc_no_empty_return' => true,
    'phpdoc_no_useless_inheritdoc' => true,
    'phpdoc_return_self_reference' => true,
    'phpdoc_scalar' => true,
    'phpdoc_separation' => true,
    'phpdoc_single_line_var_spacing' => true,
    'phpdoc_trim_consecutive_blank_line_separation' => true,
    'phpdoc_trim' => true,
    'phpdoc_types' => true,

    // Return Notation
    'no_useless_return' => true,
    'return_assignment' => true,
    'simplified_null_return' => true,

    // Semicolon
    'multiline_whitespace_before_semicolons' => [
        'strategy' => 'new_line_for_chained_calls',
    ],
    'no_empty_statement' => true,
    'no_singleline_whitespace_before_semicolons' => true,
    'semicolon_after_instruction' => true,
    'space_after_semicolon' => [
        'remove_in_empty_for_expressions' => true,
    ],

    // Strict
    'declare_strict_types' => true,

    // String Notation
    'explicit_string_variable' => true,
    'heredoc_closing_marker' => true,
    'heredoc_to_nowdoc' => true,
    'multiline_string_to_heredoc' => true,
    'no_trailing_whitespace_in_string' => true,
    'simple_to_complex_string_variable' => true,
    'single_quote' => true,
    'string_length_to_empty' => true,
    'string_line_ending' => true,

    // Whitespace
    'array_indentation' => true,
    'blank_line_before_statement' => [
        'statements' => [
            'case',
            'declare',
            'try',
        ],
    ],
    'blank_line_between_import_groups' => true,
    'compact_nullable_type_declaration' => true,
    'indentation_type' => true,
    'line_ending' => true,
    'method_chaining_indentation' => true,
    'no_extra_blank_lines' => [
        'tokens' => [
            'extra',
            'throw',
            'use',
            'use_trait',
        ],
    ],
    'no_trailing_whitespace' => true,
    'no_whitespace_in_blank_line' => true,
    'single_blank_line_at_eof' => true,
    'spaces_inside_parentheses' => true,
    'statement_indentation' => true,
    'type_declaration_spaces' => true,
    'types_spaces' => [
        'space' => 'none',
    ],
];

return (new Config)
    ->setParallelConfig(PhpCsFixer\Runner\Parallel\ParallelConfigFactory::detect())
    ->setRiskyAllowed(true)
    ->setRules($rules)
    ->setFinder($finder)
;
