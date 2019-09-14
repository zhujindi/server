/* Copyright (c) 2000, 2013, Oracle and/or its affiliates.
   Copyright (c) 2008, 2017, MariaDB Corporation.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335  USA */


#include "mariadb.h"
#include "sql_select.h"
#include "opt_trace.h"

int test_if_order_by_key(JOIN *join, ORDER *order, TABLE *table, uint idx,
                         uint *used_key_parts= NULL);
COND* substitute_for_best_equal_field(THD *thd, JOIN_TAB *context_tab,
                                      COND *cond,
                                      COND_EQUAL *cond_equal,
                                      void *table_join_idx,
                                      bool do_substitution);
enum_nested_loop_state
end_nest_materialization(JOIN *join, JOIN_TAB *join_tab, bool end_of_records);
bool get_range_limit_read_cost(const JOIN_TAB *tab, const TABLE *table,
                               ha_rows table_records, uint keynr,
                               ha_rows rows_limit, double *read_time);
Item **get_sargable_cond(JOIN *join, TABLE *table);


/*
  @brief
    Substitute field items of tables inside the sort-nest with sort-nest's
    field items.

  @details
    Substitute field items of tables inside the sort-nest with sort-nest's
    field items. This is needed for expressions which would be
    evaluated in the post ORDER BY context.

    Example:

      SELECT * FROM t1, t2, t3
      WHERE t1.a = t2.a AND t2.b = t3.b AND t1.c > t3.c
      ORDER BY t1.a,t2.c
      LIMIT 5;

    Let's say in this case the join order is t1,t2,t3 and there is a sort-nest
    on the prefix t1,t2.

    Now looking at the WHERE clause, splitting it into 2 parts:
      (1) t2.b = t3.b AND t1.c > t3.c   ---> condition external to the nest
      (2) t1.a = t2.a                   ---> condition internal to the nest

    Now look at the condition in (1), this would be evaluated in the post
    ORDER BY context.

    So t2.b and t1.c should actually refer to the sort-nest's field items
    instead of field items of the tables inside the sort-nest.
    This is why we need to substitute field items of the tables inside the
    sort-nest with sort-nest's field items.

    For the condition in (2) there is no need for substitution as this condition
    is internal to the nest and would be evaluated before we do the sorting
    for the sort-nest.

    This function does the substitution for
      - WHERE clause
      - SELECT LIST
      - ORDER BY clause
      - ON expression
      - REF access items
*/

void JOIN::substitute_base_with_nest_field_items()
{
  REPLACE_NEST_FIELD_ARG arg= {this};
  List_iterator<Item> it(fields_list);
  Item *item, *new_item;

  /* Substituting SELECT list field items with sort-nest's field items */
  while ((item= it++))
  {
    if ((new_item= item->transform(thd,
                                   &Item::replace_with_nest_items, TRUE,
                                   (uchar *) &arg)) != item)
    {
      new_item->name= item->name;
      thd->change_item_tree(it.ref(), new_item);
    }
    new_item->update_used_tables();
  }

  /* Substituting ORDER BY field items with sort-nest's field items */
  ORDER *ord;
  for (ord= order; ord ; ord=ord->next)
  {
    (*ord->item)= (*ord->item)->transform(thd,
                                          &Item::replace_with_nest_items,
                                          TRUE, (uchar *) &arg);
    (*ord->item)->update_used_tables();
  }

  JOIN_TAB *end_tab= sort_nest_info->nest_tab;
  uint i, j;
  for (i= const_tables + sort_nest_info->n_tables, j=0;
       i < top_join_tab_count; i++, j++)
  {
    JOIN_TAB *tab= end_tab + j;

    if (tab->type == JT_REF || tab->type == JT_EQ_REF ||
        tab->type == JT_REF_OR_NULL)
      substitute_ref_items(tab);

    /* Substituting ON-EXPR field items with sort-nest's field items */
    if (*tab->on_expr_ref)
    {
      item= (*tab->on_expr_ref)->transform(thd,
                                           &Item::replace_with_nest_items,
                                           TRUE, (uchar *) &arg);
      *tab->on_expr_ref= item;
      (*tab->on_expr_ref)->update_used_tables();
    }

    /*
      Substituting REF field items for SJM lookup with sort-nest's field items
    */
    if (tab->bush_children)
      substitutions_for_sjm_lookup(tab);
  }

  extract_condition_for_the_nest();

  /* Substituting WHERE clause's field items with sort-nest's field items */
  if (conds)
  {
    conds= conds->transform(thd, &Item::replace_with_nest_items, TRUE,
                            (uchar *) &arg);
    conds->update_used_tables();
  }
}

/*
  @brief
    Substitute ref access field items with sort-nest field items.

  @param tab                join tab structure having ref access

*/

void JOIN::substitute_ref_items(JOIN_TAB *tab)
{
  REPLACE_NEST_FIELD_ARG arg= {this};
  Item *item;
  /* Substituting REF field items with sort-nest's field items */
  for (uint keypart= 0; keypart < tab->ref.key_parts; keypart++)
  {
    item= tab->ref.items[keypart]->transform(thd,
                                             &Item::replace_with_nest_items,
                                             TRUE, (uchar *) &arg);
    if (item != tab->ref.items[keypart])
    {
      tab->ref.items[keypart]= item;
      Item *real_item= item->real_item();
      store_key *key_copy= tab->ref.key_copy[keypart];
      if (key_copy->type() == store_key::FIELD_STORE_KEY)
      {
        store_key_field *field_copy= ((store_key_field *)key_copy);
        DBUG_ASSERT(real_item->type() == Item::FIELD_ITEM);
        field_copy->change_source_field((Item_field *) real_item);
      }
    }
  }
}

/*
  @brief
    Substitute the left expression of the IN subquery with sort-nest
    field items.

  @param sjm_tab                SJM lookup join tab

  @details
    This substitution is needed for SJM lookup when the SJM materialized
    table is outside the sort-nest.

    For example:
      SELECT t1.a, t2.a
      FROM  t1, t2
      WHERE ot1.a in (SELECT it.b FROM it) AND ot1.b = t1.b
      ORDER BY t1.a desc, ot1.a desc
      LIMIT 5;

    Lets consider the join order here is t1, t2, <subquery2> and there is a
    sort-nest on t1, t2. For <subquery2> we do SJM lookup.
    So for the SJM table there would be a ref access created on the condition
    t2.a=it.b. But as one can see table t2 is inside the sort-nest and the
    condition t2.a=it.b can only be evaluated in the post ORDER BY context,
    so we need to substitute t2.a with the corresponding field item in the
    sort-nest.

*/

void JOIN::substitutions_for_sjm_lookup(JOIN_TAB *sjm_tab)
{
  JOIN_TAB *tab= sjm_tab->bush_children->start;
  TABLE_LIST *emb_sj_nest= tab->table->pos_in_table_list->embedding;

  /*
    @see setup_sj_materialization_part1
  */
  while (!emb_sj_nest->sj_mat_info)
    emb_sj_nest= emb_sj_nest->embedding;
  SJ_MATERIALIZATION_INFO *sjm= emb_sj_nest->sj_mat_info;

  if (!sjm->is_sj_scan)
  {
    Item *left_expr= emb_sj_nest->sj_subq_pred->left_expr;
    REPLACE_NEST_FIELD_ARG arg= {this};
    left_expr= left_expr->transform(thd, &Item::replace_with_nest_items,
                                    TRUE, (uchar *)&arg);
    left_expr->update_used_tables();
    emb_sj_nest->sj_subq_pred->left_expr= left_expr;
  }
}


/*
  @brief
    Extract from the WHERE clause the sub-condition which is internal to the
    sort-nest

  @param join          the join handler

  @details
    Extract the sub-condition from the WHERE clause that can be added to the
    tables inside the sort-nest and can be evaluated before the sorting is done
    for the sort-nest.

  Example
    SELECT * from t1,t2,t3
    WHERE t1.a > t2.a        (1)
     AND  t2.b = t3.b        (2)
    ORDER BY t1.a,t2.a
    LIMIT 5;

    let's say in this case the join order is t1,t2,t3 and there is a sort-nest
    on t1,t2

    From the WHERE clause we would like to extract the condition that depends
    only on the inner tables of the sort-nest. The condition (1) here satisfies
    this criteria so it would be extracted from the WHERE clause.
    The extracted condition here would be t1.a > t2.a.

    The extracted condition is stored inside the SORT_NEST_INFO structure.
    Also we remove the top level conjuncts of the WHERE clause that were
    present in the extracted condition.

    So after removal the final results would be:
      WHERE clause:    t2.b = t3.b         ----> condition external to the nest
      extracted cond:  t1.a > t2.a         ----> condition internal to the nest

*/

void JOIN::extract_condition_for_the_nest()
{
  DBUG_ASSERT(sort_nest_info);
  Item *orig_cond= conds;
  Item *extracted_cond;
  SELECT_LEX* sl= select_lex;

  /*
    check_cond_extraction_for_nest would set NO_EXTRACTION_FL for
    all the items that cannot be added to the inner tables of the nest.
    TODO varun:
      -try replacing it with check_pushable_cond, looks similar
      -change name of pushable_cond_checker_for_derived
  */
  CHECK_PUSHDOWN_FIELD_ARG arg= {sort_nest_info->nest_tables_map, TRUE};

  orig_cond->check_cond_extraction_for_grouping_fields(&Item::pushable_cond_checker_for_tables,
                                                       (uchar*)&arg);
  /*
    build_cond_for_grouping_fields would create the entire
    condition that would be added to the tables inside the nest.
    This may clone some items too.
  */

  /*
    TODO varun: need to re-factor this too, we need to make this function
    part of the Item class , no need to have it in SELECT LEX
  */
  extracted_cond= sl->build_cond_for_grouping_fields(thd, orig_cond, TRUE);

  if (extracted_cond)
  {
    if (extracted_cond->fix_fields_if_needed(thd, 0))
      return;
    extracted_cond->update_used_tables();
    /*
      Remove from the WHERE clause all the conditions that were added
      to the inner tables of the sort nest
    */
    orig_cond= remove_pushed_top_conjuncts(thd, orig_cond);
    sort_nest_info->nest_cond= extracted_cond;
  }
  conds= orig_cond;
}


/*
  Propgate all the multiple equalites for the order by items,
  so that one can use them to generate QEP that would
  also take into consideration equality propagation.

  Example
    select * from t1,t2 where t1.a=t2.a order by t1.a

  So the possible join orders would be:

  t1 join t2 then sort
  t2 join t1 then sort
  t1 sort(t1) join t2
  t2 sort(t2) join t1 => this is only possible when equality propagation is
                         performed

  @param join           JOIN handler
  @param sort_order     the ORDER BY clause
*/

void propagate_equal_field_for_orderby(JOIN *join, ORDER *first_order)
{
  if (!join->sort_nest_possible)
    return;
  ORDER *order;
  for (order= first_order; order; order= order->next)
  {
    if (optimizer_flag(join->thd, OPTIMIZER_SWITCH_ORDERBY_EQ_PROP) &&
        join->cond_equal)
    {
      Item *item= order->item[0];
      /*
        TODO: equality substitution in the context of ORDER BY is
        sometimes allowed when it is not allowed in the general case.
        We make the below call for its side effect: it will locate the
        multiple equality the item belongs to and set item->item_equal
        accordingly.
      */
      (void)item->propagate_equal_fields(join->thd,
                                         Value_source::
                                         Context_identity(),
                                         join->cond_equal);
    }
  }
}


/*
  Checks if by considering the current join_tab
  would the prefix of the join order satisfy
  the ORDER BY clause.

  @param join             JOIN handler
  @param join_tab         joined table to check if addition of this
                          table in the join order would achieve
                          the ordering
  @param previous_tables  table_map for all the tables in the prefix
                          of the current partial plan

  @retval
   TRUE   ordering is achieved with the addition of new table
   FALSE  ordering not achieved
*/

bool check_join_prefix_contains_ordering(JOIN *join, JOIN_TAB *tab,
                                         table_map previous_tables)
{
  ORDER *order;
  for (order= join->order; order; order= order->next)
  {
    Item *order_item= order->item[0];
    table_map order_tables=order_item->used_tables();
    if (!(order_tables & ~previous_tables) ||
         (order_item->excl_dep_on_tables(previous_tables | tab->table->map,
                                         FALSE)))
      continue;
    else
      return FALSE;
  }
  return TRUE;
}


/*
  @brief
    Check if the best plan has a sort-nest or not.

  @param
    n_tables[out]             set to the number of tables inside the sort-nest

  @details
    This function walks through the JOIN::best_positions array
    which holds the best plan and checks if there is prefix for
    which the join planner had picked a sort-nest.

    Also this function computes a table map for tables that allow
    join buffering. When a sort-nest is found tables following
    the sort-nest cannout use join buffering, as join buffering
    does not ensure that ordering will be mainitained.

  @retval
    TRUE  sort-nest present
    FALSE no sort-nest present
*/

bool JOIN::check_if_sort_nest_present(uint* n_tables)
{
  uint tablenr;
  table_map tables_with_buffering_allowed= 0;
  bool sort_nest_found= FALSE;
  uint tables= 0;
  for (tablenr=const_tables ; tablenr < table_count ; tablenr++)
  {
    tables++;
    POSITION *pos= &best_positions[tablenr];
    if (pos->sj_strategy == SJ_OPT_MATERIALIZE ||
        pos->sj_strategy == SJ_OPT_MATERIALIZE_SCAN)
    {
      SJ_MATERIALIZATION_INFO *sjm= pos->table->emb_sj_nest->sj_mat_info;
      for (uint j= 1; j < sjm->tables; j++)
      {
        JOIN_TAB *tab= (pos+j)->table;
        tables_with_buffering_allowed|= tab->table->map;
      }
      tablenr+= (sjm->tables-1);
    }
    else
    {
      if (!sort_nest_found)
        tables_with_buffering_allowed|= pos->table->table->map;
    }

    if (pos->sort_nest_operation_here)
    {
      DBUG_ASSERT(!sort_nest_found);
      sort_nest_found= TRUE;
      *n_tables= tables;
    }
  }
  return sort_nest_found;
}


/*
  @brief
    Create a sort nest info structure, that would store all the details
    about the sort-nest.

  @param n_tables         number of tables inside the sort-nest

  @retval
    FALSE     successful in creating the sort-nest info structure
    TRUE      error
*/

bool JOIN::create_sort_nest_info(uint n_tables)
{
  if (!(sort_nest_info= new SORT_NEST_INFO()))
    return TRUE;
  sort_nest_info->n_tables= n_tables;
  sort_nest_info->index_used= -1;
  return sort_nest_info == NULL;
}


/*
  @brief
    Make the sort-nest.

  @param join          the join handler

  @details
    Setup execution structures for sort-nest materialization:
      - Create the list of Items that are needed by the sort-nest
      - Create the materialization temporary table for the sort-nest

    This function fills up the SORT_NEST_INFO structure

  @retval
    TRUE   : In case of error
    FALSE  : Nest creation successful
*/

bool JOIN::make_sort_nest()
{
  Field_iterator_table field_iterator;

  JOIN_TAB *j;
  JOIN_TAB *tab;
  sort_nest_info->nest_tables_map= 0;

  if (unlikely(thd->trace_started()))
    add_sort_nest_tables_to_trace(this);

  /*
    List of field items of the tables inside the sort-nest is created for
    the field items that are needed to be stored inside the temporary table
    of the sort-nest. Currently Item_field objects are created for the tables
    inside the sort-nest for all the  fields which have bitmap read_set
    set for them.

    TODO varun:
    An improvement would be if to remove the fields from this
    list that are completely internal to the nest because such
    fields would not be used in computing expression in the post
    ORDER BY context
  */

  for (j= join_tab + const_tables; j < sort_nest_info->nest_tab; j++)
  {
    sort_nest_info->nest_tables_map|= j->table->map;
    if (j->bush_children)
    {
      TABLE_LIST *emb_sj_nest;
      JOIN_TAB *child_tab= j->bush_children->start;
      emb_sj_nest= child_tab->table->pos_in_table_list->embedding;

      /*
        @see setup_sj_materialization_part1
      */
      while (!emb_sj_nest->sj_mat_info)
        emb_sj_nest= emb_sj_nest->embedding;
      Item_in_subselect *item_sub= emb_sj_nest->sj_subq_pred;
      SELECT_LEX *subq_select= item_sub->unit->first_select();
      List_iterator_fast<Item> li(subq_select->item_list);
      Item *item;
      while((item= li++))
        sort_nest_info->nest_base_table_cols.push_back(item, thd->mem_root);
    }
    else
    {
      TABLE *table= j->table;
      field_iterator.set_table(table);
      for (; !field_iterator.end_of_fields(); field_iterator.next())
      {
        Field *field= field_iterator.field();
        if (!bitmap_is_set(table->read_set, field->field_index))
          continue;
        Item *item;
        if (!(item= field_iterator.create_item(thd)))
          return TRUE;
        sort_nest_info->nest_base_table_cols.push_back(item, thd->mem_root);
      }
    }
  }

  ORDER *ord;
  /*
    Substitute the ORDER by items with the best field so that equality
    propagation considered during best_access_path can be used.
  */
  for (ord= order; ord; ord=ord->next)
  {
    Item *item= ord->item[0];
    item= substitute_for_best_equal_field(thd, NO_PARTICULAR_TAB, item,
                                          cond_equal,
                                          map2table, true);
    item->update_used_tables();
    ord->item[0]= item;
  }

  tab= sort_nest_info->nest_tab;
  DBUG_ASSERT(!tab->table);

  uint sort_nest_elements= sort_nest_info->nest_base_table_cols.elements;
  sort_nest_info->tmp_table_param.init();
  sort_nest_info->tmp_table_param.bit_fields_as_long= TRUE;
  sort_nest_info->tmp_table_param.field_count= sort_nest_elements;
  sort_nest_info->tmp_table_param.force_not_null_cols= FALSE;

  const LEX_CSTRING order_nest_name= { STRING_WITH_LEN("sort-nest") };
  if (!(tab->table= create_tmp_table(thd, &sort_nest_info->tmp_table_param,
                                     sort_nest_info->nest_base_table_cols,
                                     (ORDER*) 0,
                                     FALSE /* distinct */,
                                     0, /*save_sum_fields*/
                                     thd->variables.option_bits |
                                     TMP_TABLE_ALL_COLUMNS,
                                     HA_POS_ERROR /*rows_limit */,
                                     &order_nest_name)))
    return TRUE; /* purecov: inspected */

  tab->table->map= sort_nest_info->nest_tables_map;
  sort_nest_info->table= tab->table;
  tab->type= JT_ALL;
  tab->table->reginfo.join_tab= tab;

  /*
    The list of temp table items created here, these are needed for the
    substitution for items that would be evaluated in POST SORT NEST context
  */
  field_iterator.set_table(tab->table);
  for (; !field_iterator.end_of_fields(); field_iterator.next())
  {
    Field *field= field_iterator.field();
    Item *item;
    if (!(item= new (thd->mem_root)Item_temptable_field(thd, field)))
      return TRUE;
    sort_nest_info->nest_temp_table_cols.push_back(item, thd->mem_root);
  }

  /* Setting up the scan on the temp table */
  tab->read_first_record= join_init_read_record;
  tab->read_record.read_record_func= rr_sequential;
  tab[-1].next_select= end_nest_materialization;
  sort_nest_info->materialized= FALSE;

  return FALSE;
}


/*
  Calculate the cost of adding a sort-nest to the join.

  SYNOPSIS

  sort_nest_oper_cost()
    @param join          the join handler

  @param
    join                the join handler
    join_record_count   the cardinalty of the partial join
    rec_len             length of the record in the sort-nest table

  DESCRIPTION
    The calculation for the cost of the sort-nest is done here, the cost
    included three components
      - Filling the sort-nest table
      - Sorting the sort-nest table
      - Reading from the sort-nest table

*/

double sort_nest_oper_cost(JOIN *join, double join_record_count,
                           ulong rec_len, uint idx)
{
  THD *thd= join->thd;
  double cost= 0;
  set_if_bigger(join_record_count, 1);
  /*
    The sort-nest table is not created for sorting when one does sorting
    on the first non-const table. So for this case we don't need to add
    the cost of filling the table.
  */
  if (idx != join->const_tables)
    cost=  get_tmp_table_write_cost(thd, join_record_count,rec_len) *
           join_record_count;   // cost to fill tmp table

  cost+= get_tmp_table_lookup_cost(thd, join_record_count,rec_len) *
         join_record_count;   // cost to perform post join operation used here
  cost+= get_tmp_table_lookup_cost(thd, join_record_count, rec_len) +
         (join_record_count == 0 ? 0 :
          join_record_count * log2 (join_record_count)) *
         SORT_INDEX_CMP_COST;             // cost to perform  sorting
  return cost;
}


/*
  @brief
    Calculate the number of records that would be read from the sort-nest.

  @param n_tables          number of tables in the sort-nest

  @details
    The number of records read from the sort-nest would be:

      cardinality(join of inner table of nest) * selectivity_of_limit;

    Here selectivity of limit is how many records we would expect in the output.
    selectivity_of_limit= limit / cardinality(join of all tables)

    This number of records is what we would also see in the EXPLAIN output
    for the sort-nest in the columns "rows".

  @retval Number of records that the optimizer expects to be read
          from the sort-nest
*/

double JOIN::calculate_record_count_for_sort_nest(uint n_tables)
{
  double sort_nest_records= 1, record_count;
  JOIN_TAB *tab= join_tab + const_tables;
  for (uint j= 0; j < n_tables ; j++, tab++)
  {
    record_count= tab->records_read * tab->cond_selectivity;
    sort_nest_records= COST_MULT(sort_nest_records, record_count);
  }
  sort_nest_records= sort_nest_records * fraction_output_for_nest;
  set_if_bigger(sort_nest_records, 1);
  return sort_nest_records;
}


/*
  @brief
  Find all keys for the table inside join_tab that would satisfy
  the ORDER BY clause
*/

void find_keys_that_can_achieve_ordering(JOIN *join, JOIN_TAB *tab)
{
  if (!join->sort_nest_possible)
    return;
  TABLE* table= tab->table;
  key_map keys_with_ordering;
  keys_with_ordering.clear_all();
  for (uint index= 0; index < table->s->keys; index++)
  {
    if (table->keys_in_use_for_query.is_set(index) &&
        test_if_order_by_key(join, join->order, table, index))
      keys_with_ordering.set_bit(index);
  }
  table->keys_in_use_for_order_by.intersect(keys_with_ordering);
}

/*
  @brief
  Checks if the partial plan needs filesort for ordering or an index
  picked by best_access_path achieves the ordering

  @retval
    TRUE  : Filesort is needed
    FALSE : index access satifies the ordering
*/

bool needs_filesort(JOIN_TAB *tab, uint idx, int index_used)
{
  JOIN *join= tab->join;
  if (idx && idx == join->const_tables)
    return TRUE;

  TABLE *table= tab->table;
  if (index_used >= 0 && index_used < MAX_KEY)
   return !table->keys_in_use_for_order_by.is_set(index_used);
  return TRUE;
}

/*
  @brief
  Check if an index(used for index scan or range scan) can achieve the
  ordering for the first non-const table, if yes calculate the cost and
  check if we get a better cost than previous access chosen

  @retval
    -1  no cheaper index found for ordering
    >=0 cheaper index found for ordering
*/

int get_best_index_for_order_by_limit(JOIN_TAB *tab, double *read_time,
                                      double *records, double cardinality,
                                      int index_used, uint idx)
{
  TABLE *table= tab->table;
  JOIN *join= tab->join;
  THD *thd= join->thd;
  double save_read_time= *read_time;
  double save_records= *records;
  /**
    Cases when there is no need to consider the indexes that achieve the
    ordering

    1) If there is no limit
    2) If there is no order by clause
    3) Check index for ordering only for the first non-const table
    4) Cardinality is DBL_MAX, then we don't need to consider the index,
       it is sent to DBL_MAX for semi-join strategies
    5) Force index is used
    6) Sort nest is possible and it is not disabled(done when we want to
       get the cardinality)
    7) If there is no index that achieves the ordering
    8) Already an access method is picked that satisfies the ordering

    Do we need to consider non-covering keys that have no range access?
    Currently all indexes that satisfy the ordering are considered
  */
  if (join->select_limit == HA_POS_ERROR ||
      !join->order ||
      idx ||
      cardinality == DBL_MAX ||
      table->force_index ||
      !join->sort_nest_possible ||
      join->get_cardinality_estimate ||
      table->keys_in_use_for_order_by.is_clear_all())
    return -1;

  Json_writer_object trace_index_for_ordering(thd);
  double est_records= *records;
  double fanout= cardinality / est_records;
  int best_index=-1;
  ha_rows table_records= table->stat_records();
  Json_writer_array considered_indexes(thd, "considered_indexes");
  for (uint idx= 0 ; idx < table->s->keys; idx++)
  {
    if (!table->keys_in_use_for_order_by.is_set(idx))
      continue;
    Json_writer_object possible_key(thd);
    KEY *keyinfo= table->key_info + idx;
        possible_key.add("index", keyinfo->name);
    double rec_per_key, index_scan_time;
    ha_rows select_limit= join->select_limit;
    select_limit= (ha_rows) (select_limit < fanout ?
                             1 : select_limit/fanout);

    est_records= MY_MIN(est_records, ha_rows(table_records *
                                             table->cond_selectivity));
    if (select_limit > est_records)
      select_limit= table_records;
    else
      select_limit= (ha_rows) (select_limit * (double) table_records /
                               est_records);
    possible_key.add("updated_limit", select_limit);
    rec_per_key= keyinfo->actual_rec_per_key(keyinfo->user_defined_key_parts-1);
    set_if_bigger(rec_per_key, 1);
    index_scan_time= select_limit/rec_per_key *
                     MY_MIN(rec_per_key, table->file->scan_time());
    possible_key.add("index_scan_time", index_scan_time);
    double range_scan_time;

    if (get_range_limit_read_cost(tab, table, table_records, idx,
                                  select_limit, &range_scan_time))
    {
      possible_key.add("range_scan_time", range_scan_time);
      if (range_scan_time < index_scan_time)
        index_scan_time= range_scan_time;
    }

    if (index_scan_time < *read_time)
    {
      best_index= idx;
      *read_time= index_scan_time;
      *records= select_limit;
    }
  }
  considered_indexes.end();
  trace_index_for_ordering.add("best_index",
                                static_cast<ulonglong>(best_index));
  trace_index_for_ordering.add("records", *records);
  trace_index_for_ordering.add("best_cost", *read_time);

  /*
    If an index already found satisfied the ordering and we picked index
    scan then revert the cost and stick with the access picked first.
    Index scan would not help in comparision with ref access.
  */
  if (index_satisfies_ordering(tab, index_used))
  {
    if (!table->quick_keys.is_set(static_cast<uint>(index_used)))
    {
      best_index= -1;
      *records= save_records;
      *read_time= save_read_time;
    }
  }
  return best_index;
}


/*
  @brief
   Disable join buffering for all the tables that come after the inner
   table of the sort-nest. This is done because join-buffering does not
   ensure ordering.
*/

bool check_if_join_buffering_needed(JOIN *join, JOIN_TAB *tab)
{
  JOIN_TAB *end= join->join_tab+join->top_join_tab_count;
  JOIN_TAB *start= join->sort_nest_info->nest_tab;
  for (JOIN_TAB *j= start; j < end; j++)
  {
    if (j == tab)
      return FALSE;
  }
  return TRUE;
}


/*
  @brief
  Check if an index satisfies the ORDER BY clause or not

  @retval
    TRUE index satisfies the ordering
    FALSE index does not satisfy the ordering
*/

bool index_satisfies_ordering(JOIN_TAB *tab, int index_used)
{
  TABLE *table= tab->table;
  if (index_used >=0 && index_used < MAX_KEY &&
      !table->keys_in_use_for_order_by.is_clear_all())
  {
    if (table->keys_in_use_for_order_by.is_set(static_cast<uint>(index_used)))
      return TRUE;
  }
  return FALSE;
}


bool setup_range_scan(JOIN *join, JOIN_TAB *tab, uint idx, double records)
{
    SQL_SELECT *sel= NULL;
    Item **sargable_cond= get_sargable_cond(join, tab->table);
    int err, rc, direction;
    uint used_key_parts;
    key_map keymap_for_range;
    THD *thd= join->thd;
    Json_writer_array forcing_range(thd, "range_scan_for_order_by_limit");

    /*
      TODO(varun)
      Find a workaround for this, alreay created a QUICK object, can't
      we just use the same object even if the reversing is required.
    */
    if (tab->quick)
    {
      delete tab->quick;
      tab->quick= 0;
    }

    sel= make_select(tab->table, join->const_table_map,
                     join->const_table_map,
                     *sargable_cond, (SORT_INFO*) 0, 1, &err);
    if (!sel)
      goto use_filesort;


    keymap_for_range.clear_all();  // Force the creation of quick select
    keymap_for_range.set_bit(idx); // only for new_ref_key.

    rc= sel->test_quick_select(join->thd, keymap_for_range,
                               (table_map) 0,
                               (ha_rows) HA_POS_ERROR,
                               true, false, true, true);
    if (rc <= 0)
      goto use_filesort;

    direction= test_if_order_by_key(join, join->order, tab->table, idx,
                                    &used_key_parts);
    if (direction == -1)
    {
      QUICK_SELECT_I *reverse_quick;
      if (sel && sel->quick)
      {
        reverse_quick= sel->quick->make_reverse(used_key_parts);
        sel->set_quick(reverse_quick);
      }
    }
    tab->quick= sel->quick;
    /*
      Fix for explain, the records here should be set to the value
      which was stored in the POSITION object as the fraction which we
      would read would have been applied.
    */
    if (records < tab->quick->records)
      tab->quick->records= records;
    sel->quick= 0;

  use_filesort:
    delete sel;
  return rc <=0 ? FALSE : TRUE;

}


/*
  @brief
    Setup range or index scan for the ordering
*/

void setup_index_use_for_ordering(JOIN *join, int index_no)
{
  SORT_NEST_INFO *sort_nest_info= join->sort_nest_info;
  sort_nest_info->nest_tab= join->join_tab + join->const_tables;
  POSITION *cur_pos= &join->best_positions[join->const_tables];
  index_no= (index_no == -1) ?
            (cur_pos->table->quick ? cur_pos->table->quick->index : -1) :
            index_no;
  if (index_satisfies_ordering(cur_pos->table, index_no))
  {
    if (cur_pos->table->table->quick_keys.is_set(index_no))
    {
      // Range scan
      (void)setup_range_scan(join, cur_pos->table, index_no,
                             cur_pos->records_read);
      sort_nest_info->index_used= -1;
    }
    else
    {
      if (cur_pos->table->quick)
      {
        delete cur_pos->table->quick;
        cur_pos->table->quick= 0;
      }
      sort_nest_info->index_used= index_no; // Index scan
    }
  }
  else
    sort_nest_info->index_used= -1;
}


int get_index_on_table(JOIN_TAB *tab)
{
  int idx= -1;
  if (tab->type == JT_REF  || tab->type == JT_EQ_REF ||
      tab->type == JT_REF_OR_NULL)
    idx= tab->ref.key;
  else if (tab->type == JT_NEXT)
    idx= tab->index;
  else if (tab->type == JT_ALL &&
           tab->select && tab->select->quick)
    idx= tab->select->quick->index;

  return idx;
}

/*
  @brief
    Calculate the selectivity of limit.

  @description
    The selectivity that we get is used to make an estimate of rows
    that we would read from the partial join of the tables inside the
    sort-nest.
  @param
    join                      the join handler
    cardinality[out]          set the output cardinality of the join
*/

void set_fraction_output_for_nest(JOIN *join, double *cardinality)
{
  if (join->sort_nest_possible && !join->get_cardinality_estimate)
  {
    double total_rows= join->join_record_count;
    set_if_bigger(total_rows, 1);
    *cardinality= total_rows;
    join->fraction_output_for_nest= join->select_limit < total_rows ?
                                    (join->select_limit / total_rows) :
                                     1.0;
    Json_writer_object trace_cardinality(join->thd);
    trace_cardinality.add("cardinality", total_rows);
    trace_cardinality.add("selectivity_of_limit",
                          join->fraction_output_for_nest*100);

  }
}


/*
  Sort nest is needed currently when one can shortcut the join execution.

  So for the operations where one requires entire join computation to be
  done first and then apply the operation on the join output,
  such operations can't make use of the sort-nest.
  So this function disables the use of sort-nest for such operations.

  Sort nest is not allowed for
  1) ORDER clause is not present
  2) Only constant tables in the join
  3) DISTINCT clause
  4) GROUP BY CLAUSE
  5) HAVING clause (that was not pushed to where)
  6) Aggregate Functions
  7) Window Functions
  8) Using ROLLUP
  9) Using SQL_BUFFER_RESULT
  10) LIMIT is present
  11) Only Select queries can use the sort nest

  Returns
   TRUE if sort-nest is allowed
   FALSE otherwise

*/

bool JOIN::sort_nest_allowed()
{
  return thd->variables.use_sort_nest && order &&
         !(const_tables == table_count ||
           (select_distinct || group_list) ||
           having  ||
           MY_TEST(select_options & OPTION_BUFFER_RESULT) ||
           (rollup.state != ROLLUP::STATE_NONE && select_distinct) ||
           select_lex->window_specs.elements > 0 ||
           select_lex->agg_func_used() ||
           select_limit == HA_POS_ERROR ||
           thd->lex->sql_command != SQLCOM_SELECT);
}
