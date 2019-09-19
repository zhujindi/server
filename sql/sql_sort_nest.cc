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

    For the condition in (2) there is no need for substitution as this
    condition is internal to the nest and would be evaluated before we
    do the sorting for the sort-nest.

    This function does the substitution for
      - WHERE clause
      - SELECT LIST
      - ORDER BY clause
      - ON expression
      - REF access items
*/

void JOIN::substitute_base_with_nest_field_items()
{
  List_iterator<Item> it(fields_list);
  Item *item, *new_item;

  /* Substituting SELECT list field items with sort-nest's field items */
  while ((item= it++))
  {
    if ((new_item= item->transform(thd,
                                   &Item::replace_with_nest_items, TRUE,
                                   (uchar *) this)) != item)
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
                                          TRUE, (uchar *) this);
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
                                           TRUE, (uchar *) this);
      *tab->on_expr_ref= item;
      (*tab->on_expr_ref)->update_used_tables();
    }

    /*
      Substituting REF field items for SJM lookup with sort-nest's field items
    */
    if (tab->bush_children)
      substitutions_for_sjm_lookup(tab);
  }

  /*
    This needs a pointer to the nest, so that this could be used in general
  */
  extract_condition_for_the_nest();

  /* Substituting WHERE clause's field items with sort-nest's field items */
  if (conds)
  {
    conds= conds->transform(thd, &Item::replace_with_nest_items, TRUE,
                            (uchar *) this);
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
  Item *item;
  /* Substituting REF field items with sort-nest's field items */
  for (uint keypart= 0; keypart < tab->ref.key_parts; keypart++)
  {
    item= tab->ref.items[keypart]->transform(thd,
                                             &Item::replace_with_nest_items,
                                             TRUE, (uchar *) this);
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
    left_expr= left_expr->transform(thd, &Item::replace_with_nest_items,
                                    TRUE, (uchar *) this);
    left_expr->update_used_tables();
    emb_sj_nest->sj_subq_pred->left_expr= left_expr;
  }
}


/*
  @brief
    Extract from the WHERE clause the sub-condition which is internal to the
    sort-nest

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

  /*
    check_cond_extraction_for_nest would set NO_EXTRACTION_FL for
    all the items that cannot be added to the inner tables of the nest.
    TODO varun:
      -change name to check_pushable_cond_extraction
      -please use 2 functions here, not use a structure, looks complex
  */
  CHECK_PUSHDOWN_FIELD_ARG arg= {sort_nest_info->nest_tables_map, TRUE};

  orig_cond->check_pushable_cond_extraction(&Item::pushable_cond_checker_for_tables,
                                            (uchar*)&arg);
  /*
    build_pushable_condition would create a sub-condition that would be
    added to the tables inside the nest. This may clone some items too.
  */
  extracted_cond= orig_cond->build_pushable_condition(thd, TRUE);

  if (extracted_cond)
  {
    if (extracted_cond->fix_fields_if_needed(thd, 0))
      return;
    extracted_cond->update_used_tables();
    /*
      Remove from the WHERE clause  the top level conjuncts that were
      extracted for the inner tables of the sort nest
    */
    orig_cond= remove_pushed_top_conjuncts(thd, orig_cond);
    sort_nest_info->nest_cond= extracted_cond;
  }
  conds= orig_cond;
}


/*
  @brief
    Propagate the Multiple Equalities for all the ORDER BY items.

  @details
    Propagate the multiple equalities for the ORDER BY items.
    This is needed so that we can generate different join orders
    that would satisfy ordering after taking equality propagation
    into consideration.

    Example
      SELECT * FROM t1, t2, t3
      WHERE t1.a = t2.a AND t2.b = t3.a
      ORDER BY t2.a, t3.a
      LIMIT 10;

    Possible join orders which satisfy the ORDER BY clause and
    which we can get after equality propagation are:
        - t2, sort(t2), t3, t1          ---> substitute t3.a with t2.b
        - t2, sort(t2), t1, t3          ---> substitute t3.a with t2.b
        - t1, t3, sort(t1,t3), t2       ---> substitute t2.a with t1.a
        - t1, t2, sort(t1,t2), t3       ---> substitute t3.a with t2.b

    So with equality propagation for ORDER BY items, we can get more
    join orders that could satisfy the ORDER BY clause.
*/

void JOIN::propagate_equal_field_for_orderby()
{
  if (!sort_nest_possible)
    return;
  ORDER *ord;
  for (ord= order; ord; ord= ord->next)
  {
    if (optimizer_flag(thd, OPTIMIZER_SWITCH_ORDERBY_EQ_PROP) && cond_equal)
    {
      Item *item= ord->item[0];
      /*
        TODO: equality substitution in the context of ORDER BY is
        sometimes allowed when it is not allowed in the general case.
        We make the below call for its side effect: it will locate the
        multiple equality the item belongs to and set item->item_equal
        accordingly.
      */
      (void)item->propagate_equal_fields(thd,
                                         Value_source::
                                         Context_identity(),
                                         cond_equal);
    }
  }
}


/*
  @brief
    Check whether ORDER BY items can be evaluated for a given prefix

  @param previous_tables  table_map of all the tables in the prefix
                          of the current partial plan

  @details
    Here we walk through the ORDER BY items and check if the prefix of the
    join resolves the ordering.
    Also we look at the multiple equalities for each item in the ORDER BY list
    to see if the ORDER BY items can be resolved by the given prefix.

    Example
      SELECT * FROM t1, t2, t3
      WHERE t1.a = t2.a AND t2.b = t3.a
      ORDER BY t2.a, t3.a
      LIMIT 10;

    Let's say the given prefix is table {t1,t3}, then this function would
    return TRUE because there is an equality condition t2.a=t1.a ,
    so t2.a can be resolved with t1.a. Hence the given prefix {t1,t3} would
    resolve the ORDER BY clause.

  @retval
   TRUE   ordering can be evaluated by the given prefix
   FALSE  otherwise

*/

bool JOIN::check_join_prefix_resolves_ordering(table_map previous_tables)
{
  DBUG_ASSERT(order);
  ORDER *ord;
  for (ord= order; ord; ord= ord->next)
  {
    Item *order_item= ord->item[0];
    table_map order_tables=order_item->used_tables();
    if (!(order_tables & ~previous_tables) ||
         (order_item->excl_dep_on_tables(previous_tables, FALSE)))
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
    nest_tables_map[out]      map of tables inside the sort-nest
  @details
    This function walks through the JOIN::best_positions array
    which holds the best plan and checks if there is prefix for
    which the join planner had picked a sort-nest.

    Also this function computes a table map for tables that are inside the
    sort-nest

  @retval
    TRUE  sort-nest present
    FALSE no sort-nest present
*/

bool JOIN::check_if_sort_nest_present(uint* n_tables,
                                      table_map *nest_tables_map)
{
  uint tablenr;
  table_map nest_tables= 0;
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
        nest_tables|= tab->table->map;
      }
      tablenr+= (sjm->tables-1);
    }
    else
      nest_tables|= pos->table->table->map;

    if (pos->sort_nest_operation_here)
    {
      *n_tables= tables;
      *nest_tables_map= nest_tables;
      return TRUE;
    }
  }
  return FALSE;
}


/*
  @brief
    Create a sort nest info structure

  @param n_tables          number of tables inside the sort-nest
  @param nest_tables_map   map of top level tables inside the sort-nest

  @details
    This sort-nest structure would hold all the information about the
    sort-nest.

  @retval
    FALSE     successful in creating the sort-nest info structure
    TRUE      error
*/

bool JOIN::create_sort_nest_info(uint n_tables, table_map nest_tables_map)
{
  if (!(sort_nest_info= new SORT_NEST_INFO()))
    return TRUE;
  sort_nest_info->n_tables= n_tables;
  sort_nest_info->index_used= -1;
  sort_nest_info->nest_tables_map= nest_tables_map;
  return sort_nest_info == NULL;
}


/*
  @brief
    Make the sort-nest.

  @details
    Setup execution structures for sort-nest materialization:
      - Create the list of Items of the inner tables of the sort-nest
        that are needed for the post ORDER BY computations
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
    if (!j->bush_children)
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
    else
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
  @brief
    Calculate the cost of adding a sort-nest.

  @param
    join_record_count   the cardinality of the partial join
    idx                 position of the joined table in the partial plan
    rec_len             estimate of length of the record in the sort-nest table

  @details
    The calculation for the cost of the sort-nest is done here, the cost
    includes three components
      1) Filling the sort-nest table
      2) Sorting the sort-nest table
      3) Reading from the sort-nest table
*/

double JOIN::sort_nest_oper_cost(double join_record_count, uint idx,
                                 ulong rec_len)
{
  double cost= 0;
  set_if_bigger(join_record_count, 1);
  /*
    The sort-nest table is not created for sorting when one does sorting
    on the first non-const table. So for this case we don't need to add
    the cost of filling the table.
  */
  if (idx != const_tables)
    cost=  get_tmp_table_write_cost(thd, join_record_count, rec_len) *
           join_record_count; // cost to fill temp table

  /*
    TODO varun:
    should we apply the limit here as we would read only
    join_record_count * selectivity_of_limit records
  */
  cost+= get_tmp_table_lookup_cost(thd, join_record_count, rec_len) *
         join_record_count;   // cost to perform post join operation used here
  cost+= get_tmp_table_lookup_cost(thd, join_record_count, rec_len) +
         (join_record_count == 0 ? 0 :
          join_record_count * log2 (join_record_count)) *
         SORT_INDEX_CMP_COST; // cost to perform  sorting
  return cost;
}


/*
  @brief
    Calculate the number of records that would be read from the sort-nest.

  @param n_tables          number of tables in the sort-nest

  @details
    The number of records read from the sort-nest would be:

      cardinality(join of inner table of nest) * selectivity_of_limit;

    Here selectivity of limit is how many records we would expect in the
    output.
      selectivity_of_limit= limit / cardinality(join of all tables)

    This number of records is what we would also see in the EXPLAIN output
    for the sort-nest in the columns "rows".

  @retval Number of records that the optimizer expects to be read from the
          sort-nest
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
    Find all keys that can resolve the ORDER BY clause for a table

  @param
    table                        table for which keys need to be found

  @details
    This function sets the flag TABLE::keys_with_ordering with all the
    indexes of a table that can resolve the ORDER BY clause.

  TODO varun:
    1) create a map for the the keys that can be used for order by, don't use
       the map keys_with_ordering, as this can cause problems?
*/

void JOIN::find_keys_that_can_achieve_ordering(TABLE *table)
{
  if (!sort_nest_possible)
    return;
  table->keys_with_ordering.clear_all();
  for (uint index= 0; index < table->s->keys; index++)
  {
    if (table->keys_in_use_for_query.is_set(index) &&
        test_if_order_by_key(this, order, table, index))
      table->keys_with_ordering.set_bit(index);
  }
  /*
    TODO varun:
    Is this really required, I think a hint can be given as to which index to
    use for ordering, if this is TRUE add a test case for this
  */
  table->keys_with_ordering.intersect(table->keys_in_use_for_order_by);
}


/*
  @brief
    Checks if the given prefix needs Filesort for ordering.

  @param
    table          joined table
    idx            position of the joined table in the partial plan
    index_used     >=0 number of the index that is picked as best access
                   -1  no index access chosen

  @details
    Here we check if a given prefix requires Filesort or index on the
    first non-const table to resolve the ORDER BY clause.

  @retval
    TRUE   Filesort is needed
    FALSE  index present that satisfies the ordering
*/

bool JOIN::needs_filesort(TABLE *table, uint idx, int index_used)
{
  if (idx != const_tables)
    return TRUE;

  return !check_if_index_satisfies_ordering(table, index_used);
}

/*
  @brief
    Find a cheaper index that resolves ordering on the first non-const table.

  @param
    tab                       joined table
    read_time [out]           cost for the best index picked if cheaper
    records   [out]           estimate of records going to be accessed by the
                              index
    cardinality               estimate of records in the join output
    index_used                >=0 number of index used for best access
                              -1  no index used for best access
    idx                       position of the joined table in the partial plan
  @retval
    -1  no cheaper index found for ordering
    >=0 cheaper index found for ordering

  TODO varun:
    1) one line brief please
    2) add comments about limit in the details section.
    3) Mention that limit is picked from the join structure
    4) This needs a detailed explanation, also mention it is picked from
       test_if_cheaper_ordering
*/

int get_best_index_for_order_by_limit(JOIN_TAB *tab, double *read_time,
                                      double *records, double cardinality,
                                      int index_used, uint idx)
{
  TABLE *table= tab->table;
  JOIN *join= tab->join;
  double save_read_time= *read_time;
  double save_records= *records;

  /**
    Cases when there is no need to consider the indexes that achieve the
    ordering

    1) If there is no limit
    2) Check index for ordering only for the first non-const table
    3) Cardinality is DBL_MAX, then we don't need to consider the index,
       it is sent to DBL_MAX for semi-join strategies
    4) Force index is used
    5) Sort nest is possible is not possible (would also cover the case
                                              if ORDER BY clause is present
                                              or not)
    6) join planner is run to get estimate of cardinality
    7) If there is no index that can resolve the ORDER BY clause
  */

  if (join->select_limit == HA_POS_ERROR ||                    // (1)
      idx != join->const_tables ||                             // (2)
      cardinality == DBL_MAX ||                                // (3)
      table->force_index ||                                    // (4)
      !join->sort_nest_possible ||                             // (5)
      join->get_cardinality_estimate ||                        // (6)
      table->keys_with_ordering.is_clear_all())                // (7)
    return -1;

  THD *thd= join->thd;
  Json_writer_object trace_index_for_ordering(thd);
  double est_records= *records;
  double fanout= cardinality / est_records;
  int best_index=-1;
  ha_rows table_records= table->stat_records();
  Json_writer_array considered_indexes(thd, "considered_indexes");
  for (uint idx= 0 ; idx < table->s->keys; idx++)
  {
    if (!table->keys_with_ordering.is_set(idx))
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
    If an index already found satisfied the ordering and we picked an index
    for which we choose to do index scan then revert the cost and stick
    with the access picked first.
    Index scan would not help in comparison with ref access.
  */
  if (check_if_index_satisfies_ordering(tab->table, index_used))
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
    Disallow join buffering for tables that are read after sorting is done.

  @param
    tab                      table to check if join buffering is allowed or not

  @details
    Disallow join buffering for all the tables at the top level that are read
    after sorting is done.
    There are 2 cases
      1) Sorting on the first non-const table
         For all the tables join buffering is not allowed
      2) Sorting on a prefix of the join with a sort-nest
         For the tables inside the sort-nest join buffering is allowed but
         for tables outside the sort-nest join buffering is not allowed

    Also for SJM table that come after the sort-nest, join buffering is allowed
    for the inner tables of the SJM.

  @retval
    TRUE   Join buffering is allowed
    FALSE  Otherwise
*/

bool JOIN::is_join_buffering_allowed(JOIN_TAB *tab)
{
  if (!sort_nest_info)
    return TRUE;

  // no need to disable join buffering for the inner tables of SJM
  if (tab->bush_root_tab)
    return TRUE;

  if (tab->table->map & sort_nest_info->nest_tables_map)
    return TRUE;
  return FALSE;
}


/*
  @brief
    Check if an index on the first non-const table resolves the ORDER BY clause.

  @param
    table                       First non-const table
    index_used                  index to be checked

  @retval
    TRUE  index resolves the ORDER BY clause
    FALSE otherwise
*/

bool check_if_index_satisfies_ordering(TABLE *table, int index_used)
{
  /*
    index_used is set to
    -1          for Table Scan
    MAX_KEY     for HASH JOIN
    >=0         for ref/range/index access
  */
  if (index_used < 0 || index_used == MAX_KEY)
    return FALSE;

  if (table->keys_with_ordering.is_set(static_cast<uint>(index_used)))
    return TRUE;
  return FALSE;
}


/*
  @brief
    Set up range scan for the table.

  @param
    tab                    table for which range scan needs to be setup
    idx                    index for which range scan needs to created
    records                estimate of records to be read with range scan

  @details
    Range scan is setup here for an index that can resolve the ORDER BY clause.
    There are 2 cases here:
      1) If the range scan is on the same index for which we created
         QUICK_SELECT when we ran the range optimizer earlier, then we try
         to reuse it.
      2) The range scan is on a different index then we need to create
         QUICK_SELECT for the new key. This is done by running the range
         optimizer again.

    Also here we take into account if the ordering is in reverse direction.
    For DESCENDING we try to reverse the QUICK_SELECT.

  @note
    This is done for the ORDER BY LIMIT optimization. We try to force creation
    of range scan for an index that the join planner picked for us. Also here
    we reverse the range scan if the ordering is in reverse direction.
*/

void JOIN::setup_range_scan(JOIN_TAB *tab, uint idx, double records)
{
  SQL_SELECT *sel= NULL;
  Item **sargable_cond= get_sargable_cond(this, tab->table);
  int err, rc, direction;
  uint used_key_parts;
  key_map keymap_for_range;
  Json_writer_array forcing_range(thd, "range_scan_for_order_by_limit");

  sel= make_select(tab->table, const_table_map, const_table_map,
                   *sargable_cond, (SORT_INFO*) 0, 1, &err);
  if (!sel)
    goto use_filesort;

  /*
    If the table already had a range access, check if it is the same as the
    one we wanted to create range scan for, if yes don't run the range
    optimizer again.
  */

  if (!(tab->quick && tab->quick->index == idx))
  {
    /* Free the QUICK_SELECT that was built earlier. */
    delete tab->quick;
    tab->quick= NULL;

    keymap_for_range.clear_all();  // Force the creation of quick select
    keymap_for_range.set_bit(idx); // only for index using range access.

    rc= sel->test_quick_select(thd, keymap_for_range,
                               (table_map) 0,
                               (ha_rows) HA_POS_ERROR,
                               true, false, true, true);
    if (rc <= 0)
      goto use_filesort;
  }
  else
    sel->quick= tab->quick;

  direction= test_if_order_by_key(this, order, tab->table, idx,
                                  &used_key_parts);

  if (direction == -1)
  {
    /*
      QUICK structure is reversed here as the ordering is in DESC order
    */
    QUICK_SELECT_I *reverse_quick;
    if (sel && sel->quick)
    {
      reverse_quick= sel->quick->make_reverse(used_key_parts);
      if (!reverse_quick)
        goto use_filesort;
      sel->set_quick(reverse_quick);
    }
  }

  tab->quick= sel->quick;

  /*
    Fix for explain, the records here should be set to the value
    which was stored in the JOIN::best_positions object. This is needed
    because the estimate of rows to be read for the first non-const table had
    taken selectivity of limit into account.
  */
  if (sort_nest_possible && records < tab->quick->records)
    tab->quick->records= records;

  sel->quick= NULL;

use_filesort:
  delete sel;
}


/*
  @brief
    Setup range/index scan to resolve ordering on the first non-const table.

  @param
    index_no        index for which index scan or range scan needs to be setup

  @details
    Here we try to prepare range scan or index scan for an index that can be
    used to resolve the ORDER BY clause. This is used only for the first
    non-const table of the join.

    For range scan
      There is a separate call to setup_range_scan, where the QUICK_SELECT is
      created for range access. In case we are not able to create a range
      access, we switch back to use Filesort on the first table.
      see @setup_range_scan
    For index scan
      We just store the index in SORT_NEST_INFO::index_used.
*/

void JOIN::setup_index_use_for_ordering(int index_no)
{
  DBUG_ASSERT(sort_nest_info->index_used == -1);

  sort_nest_info->nest_tab= join_tab + const_tables;
  POSITION *cur_pos= &best_positions[const_tables];
  JOIN_TAB *tab= cur_pos->table;

  index_no= (index_no == -1) ?
            (cur_pos->table->quick ? cur_pos->table->quick->index : -1) :
            index_no;

  if (check_if_index_satisfies_ordering(tab->table, index_no))
  {
    if (tab->table->quick_keys.is_set(index_no))
    {
      // Range scan
      setup_range_scan(tab, index_no, cur_pos->records_read);
      sort_nest_info->index_used= -1;
    }
    else
    {
       // Index scan
      if (tab->quick)
      {
        delete tab->quick;
        tab->quick= NULL;
      }
      sort_nest_info->index_used= index_no;
    }
  }
}


/*
  @brief
    Get index used to access the table, if present

  @retval
    >=0 index used to access the table
    -1  no index used to access table, probably table scan is done

  TODO varun:
    -consider hash join also.
*/

int JOIN_TAB::get_index_on_table()
{
  int idx= -1;

  if (type == JT_REF  || type == JT_EQ_REF || type == JT_REF_OR_NULL)
    idx= ref.key;
  else if (type == JT_NEXT)
    idx= index;
  else if (type == JT_ALL && select && select->quick)
    idx= select->quick->index;
  return idx;
}

/*
  @brief
    Calculate the selectivity of limit.

  @param
    cardinality[out]          set the output cardinality of the join

  @details
    The selectivity of limit is calculated as
        selecitivity_of_limit=  rows_in_limit / cardinality_of_join

  @note
    The selectivity that we get is used to make an estimate of rows
    that we would read from the partial join of the tables inside the
    sort-nest.
*/

void JOIN::set_fraction_output_for_nest(double *cardinality)
{
  if (sort_nest_possible && !get_cardinality_estimate)
  {
    set_if_bigger(join_record_count, 1);
    *cardinality= join_record_count;
    fraction_output_for_nest= select_limit < join_record_count ?
                              select_limit / join_record_count :
                              1.0;
    if (unlikely(thd->trace_started()))
    {
      Json_writer_object trace_limit(thd);
      trace_limit.add("cardinality", join_record_count);
      trace_limit.add("selectivity_of_limit", fraction_output_for_nest*100);
    }
  }
}


/*
  @brief
    Sort nest is allowed when one can shortcut the join execution.

  @details
    For all the operations where one requires entire join computation to be
    done first and then apply the operation on the join output,
    such operations can't make use of the sort-nest.
    So this function disables the use of sort-nest for such operations.

    Sort nest is not allowed for
    1) No ORDER BY clause
    2) Only constant tables in the join
    3) DISTINCT CLAUSE
    4) GROUP BY CLAUSE
    5) HAVING clause
    6) Aggregate Functions
    7) Window Functions
    8) Using ROLLUP
    9) Using SQL_BUFFER_RESULT
    10) LIMIT is absent
    11) Only SELECT queries can use the sort nest

  @retval
   TRUE     Sort-nest is allowed
   FALSE    Otherwise

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

/*
  @brief
    Consider adding a sort-nest on a prefix of the join

  @param prefix_tables           map of all the tables in the prefix

  @details
    This function is used during the join planning stage, where the join
    planner decides if it can add a sort-nest on a prefix of a join.
    The join planner does not add the sort-nest in the following cases:
      1) Queries where adding a sort-nest is not possible.
         see @sort_nest_allowed
      2) Join planner is run to get the cardinality of the join
      3) All inner tables of an outer join are inside the nest or outside
      4) All inner tables of a semi-join are inside the nest or outside
      5) Given prefix cannot resolve the ORDER BY clause

  @retval
    TRUE   sort-nest can be added on a prefix of a join
    FALSE  otherwise
*/

bool JOIN::consider_adding_sort_nest(table_map prefix_tables)
{
  if (!sort_nest_possible ||                        // (1)
      get_cardinality_estimate ||                   // (2)
      cur_embedding_map ||                          // (3)
      cur_sj_inner_tables)                          // (4)
    return FALSE;

  return check_join_prefix_resolves_ordering(prefix_tables);  // (5)
}


/*
  @brief
    Check if index on a table is allowed to resolve the ORDER BY clause

  @param
    table                     joined table
    idx                       position of the table in the partial plan
    index_used                index to be checked

  @details

  @retval
    TRUE    Index can be used to resolve ordering
    FALSE   Otherwise

*/

bool JOIN::is_index_with_ordering_allowed(TABLE *table, uint idx,
                                          int index_used)
{
  /*
    AN index on a table can allowed to resolve ordering in these cases:
      1) Table should be the first non-const table
      2) Query that allows the ORDER BY LIMIT optimization.
         @see sort_nest_allowed
      3) Join planner is not run to get the estimate of cardinality
      4) Index resolves the ORDER BY clause
  */
  return  idx == const_tables &&                                   // (1)
          sort_nest_possible &&                                    // (2)
          !get_cardinality_estimate &&                             // (3)
          check_if_index_satisfies_ordering(table, index_used);    // (4)
}

