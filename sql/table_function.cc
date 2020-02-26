/*
  Copyright (c) 2020, MariaDB Corporation

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335  USA
*/

#include "mariadb.h"
#include "sql_priv.h"
#include "sql_class.h" /* TMP_TABLE_PARAM */
#include "table.h"

bool open_tmp_table(TABLE *table); // from sql_select.h
ST_SCHEMA_TABLE *find_schema_table(THD *thd, const LEX_CSTRING *table_name, //from sql_show.h
                                   bool *in_plugin);
static inline ST_SCHEMA_TABLE *find_schema_table(THD *thd, const LEX_CSTRING *table_name)
{ bool unused; return find_schema_table(thd, table_name, &unused); }

#include "table_function.h"


#ifdef MDEV17399
class ha_table_function: public handler
{
public:
  ha_table_function();
  ~ha_table_function() {}
  handler *clone(const char *name, MEM_ROOT *mem_root) {}
  const char *index_type(uint inx)
  { return "HASH"; }
  /* Rows also use a fixed-size format */
  enum row_type get_row_type() const { return ROW_TYPE_FIXED; }
  ulonglong table_flags() const
  {
    return (HA_FAST_KEY_READ | HA_NO_BLOBS | HA_NULL_IN_KEY |
            HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE |
            HA_CAN_SQL_HANDLER | HA_CAN_ONLINE_BACKUPS |
            HA_REC_NOT_IN_SEQ | HA_CAN_INSERT_DELAYED | HA_NO_TRANSACTIONS |
            HA_HAS_RECORDS | HA_STATS_RECORDS_IS_EXACT | HA_CAN_HASH_KEYS);
  }
  ulong index_flags(uint inx, uint part, bool all_parts) const
  {
    return HA_ONLY_WHOLE_INDEX | HA_KEY_SCAN_NOT_ROR;
  }
  const key_map *keys_to_use_for_scanning() { return &btree_keys; }
  uint max_supported_keys()          const { return MAX_KEY; }
  uint max_supported_key_part_length() const { return MAX_KEY_LENGTH; }
  double scan_time()
  { return (double) (stats.records+stats.deleted) / 20.0+10; }
  double read_time(uint index, uint ranges, ha_rows rows)
  { return (double) rows /  20.0+1; }

  int open(const char *name, int mode, uint test_if_locked);
  int close(void);
  void set_keys_for_scanning(void);
  /*
  int write_row(const uchar * buf);
  int update_row(const uchar * old_data, const uchar * new_data);
  int delete_row(const uchar * buf);
  virtual void get_auto_increment(ulonglong offset, ulonglong increment,
                                  ulonglong nb_desired_values,
                                  ulonglong *first_value,
                                  ulonglong *nb_reserved_values);
                                  */
  int index_read_map(uchar * buf, const uchar * key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag);
  int index_read_last_map(uchar *buf, const uchar *key, key_part_map keypart_map);
  int index_read_idx_map(uchar * buf, uint index, const uchar * key,
                         key_part_map keypart_map,
                         enum ha_rkey_function find_flag);
  int index_next(uchar * buf);
  int index_prev(uchar * buf);
  int index_first(uchar * buf);
  int index_last(uchar * buf);
  int rnd_init(bool scan);
  int rnd_next(uchar *buf);
  int rnd_pos(uchar * buf, uchar *pos);
  void position(const uchar *record);
  int can_continue_handler_scan();
  int info(uint);
  int extra(enum ha_extra_function operation);
  int reset();
  /*
  int external_lock(THD *thd, int lock_type);
  int delete_all_rows(void);
  int reset_auto_increment(ulonglong value);
  int disable_indexes(uint mode);
  int enable_indexes(uint mode);
  int indexes_are_disabled(void);
  ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);
  int delete_table(const char *from);
  void drop_table(const char *name);
  int rename_table(const char * from, const char * to);
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);
  void update_create_info(HA_CREATE_INFO *create_info);

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
			     enum thr_lock_type lock_type);
  */
  int cmp_ref(const uchar *ref1, const uchar *ref2)
  {
    return memcmp(ref1, ref2, sizeof(HEAP_PTR));
  }
  bool check_if_incompatible_data(HA_CREATE_INFO *info, uint table_changes);
  int find_unique_row(uchar *record, uint unique_idx);
private:
  void update_key_stats();
};
#endif /*MDEV17399*/


class Create_json_table: public Data_type_statistics
{
  // The following members are initialized only in start()
  Field **m_from_field, **m_default_field;
  KEY_PART_INFO *m_key_part_info;
  uchar	*m_group_buff, *m_bitmaps;
  // The following members are initialized in ctor
  uint  m_alloced_field_count;
  bool  m_using_unique_constraint;
  uint m_temp_pool_slot;
  ORDER *m_group;
  bool m_distinct;
  bool m_save_sum_fields;
  ulonglong m_select_options;
  ha_rows m_rows_limit;
  uint m_hidden_field_count;       // Remove this eventually
  uint m_group_null_items;
  uint m_null_count;
  uint m_hidden_uneven_bit_length;
  uint m_hidden_null_count;
public:
  Create_json_table(const TMP_TABLE_PARAM *param,
                   ORDER *group, bool distinct, bool save_sum_fields,
                   ulonglong select_options, ha_rows rows_limit)
   :m_alloced_field_count(0),
    m_using_unique_constraint(false),
    m_temp_pool_slot(MY_BIT_NONE),
    m_group(group),
    m_distinct(distinct),
    m_save_sum_fields(save_sum_fields),
    m_select_options(select_options),
    m_rows_limit(rows_limit),
    m_hidden_field_count(param->hidden_field_count),
    m_group_null_items(0),
    m_null_count(0),
    m_hidden_uneven_bit_length(0),
    m_hidden_null_count(0)
  { }

  void add_field(TABLE *table, Field *field, uint fieldnr, bool force_not_null_cols);

  TABLE *start(THD *thd,
               TMP_TABLE_PARAM *param,
               const LEX_CSTRING *table_alias);

  bool add_schema_fields(THD *thd, TABLE *table,
                         TMP_TABLE_PARAM *param,
                         const ST_SCHEMA_TABLE &schema_table,
                         const MY_BITMAP &bitmap);

  bool finalize(THD *thd, TABLE *table, TMP_TABLE_PARAM *param,
                bool do_not_open, bool keep_row_order);
  void cleanup_on_failure(THD *thd, TABLE *table);
};


static void
setup_tmp_table_column_bitmaps(TABLE *table, uchar *bitmaps, uint field_count)
{
  uint bitmap_size= bitmap_buffer_size(field_count);

  DBUG_ASSERT(table->s->virtual_fields == 0);

  my_bitmap_init(&table->def_read_set, (my_bitmap_map*) bitmaps, field_count,
              FALSE);
  bitmaps+= bitmap_size;
  my_bitmap_init(&table->tmp_set,
                 (my_bitmap_map*) bitmaps, field_count, FALSE);
  bitmaps+= bitmap_size;
  my_bitmap_init(&table->eq_join_set,
                 (my_bitmap_map*) bitmaps, field_count, FALSE);
  bitmaps+= bitmap_size;
  my_bitmap_init(&table->cond_set,
                 (my_bitmap_map*) bitmaps, field_count, FALSE);
  bitmaps+= bitmap_size;
  my_bitmap_init(&table->has_value_set,
                 (my_bitmap_map*) bitmaps, field_count, FALSE);
  /* write_set and all_set are copies of read_set */
  table->def_write_set= table->def_read_set;
  table->s->all_set= table->def_read_set;
  bitmap_set_all(&table->s->all_set);
  table->default_column_bitmaps();
}


static void
setup_tmp_table_column_bitmaps(TABLE *table, uchar *bitmaps)
{
  setup_tmp_table_column_bitmaps(table, bitmaps, table->s->fields);
}


void Create_json_table::add_field(TABLE *table, Field *field,
                                  uint fieldnr, bool force_not_null_cols)
{
  DBUG_ASSERT(!field->field_name.str ||
              strlen(field->field_name.str) == field->field_name.length);

  if (force_not_null_cols)
  {
    field->flags|= NOT_NULL_FLAG;
    field->null_ptr= NULL;
  }

  if (!(field->flags & NOT_NULL_FLAG))
    m_null_count++;

  table->s->reclength+= field->pack_length();

  // Assign it here, before update_data_type_statistics() changes m_blob_count
  if (field->flags & BLOB_FLAG)
    table->s->blob_field[m_blob_count]= fieldnr;

  table->field[fieldnr]= field;
  field->field_index= fieldnr;

  field->update_data_type_statistics(this);
}


/**
  Create a temp table according to a field list.

  Given field pointers are changed to point at tmp_table for
  send_result_set_metadata. The table object is self contained: it's
  allocated in its own memory root, as well as Field objects
  created for table columns.
  This function will replace Item_sum items in 'fields' list with
  corresponding Item_field items, pointing at the fields in the
  temporary table, unless this was prohibited by TRUE
  value of argument save_sum_fields. The Item_field objects
  are created in THD memory root.

  @param thd                  thread handle
  @param param                a description used as input to create the table
  @param fields               list of items that will be used to define
                              column types of the table (also see NOTES)
  @param group                Create an unique key over all group by fields.
                              This is used to retrive the row during
                              end_write_group() and update them.
  @param distinct             should table rows be distinct
  @param save_sum_fields      see NOTES
  @param select_options       Optiions for how the select is run.
                              See sql_priv.h for a list of options.
  @param rows_limit           Maximum number of rows to insert into the
                              temporary table
  @param table_alias          possible name of the temporary table that can
                              be used for name resolving; can be "".
  @param do_not_open          only create the TABLE object, do not
                              open the table in the engine
  @param keep_row_order       rows need to be read in the order they were
                              inserted, the engine should preserve this order
*/

TABLE *Create_json_table::start(THD *thd,
                               TMP_TABLE_PARAM *param,
                               const LEX_CSTRING *table_alias)
{
  MEM_ROOT *mem_root_save, own_root;
  TABLE *table;
  TABLE_SHARE *share;
  uint  copy_func_count= param->func_count;
  char  *tmpname,path[FN_REFLEN];
  Field **reg_field;
  uint *blob_field;
  /* Treat sum functions as normal ones when loose index scan is used. */
  m_save_sum_fields|= param->precomputed_group_by;
  DBUG_ENTER("Create_json_table::start");
  DBUG_PRINT("enter",
             ("table_alias: '%s'  distinct: %d  save_sum_fields: %d  "
              "rows_limit: %lu  group: %d", table_alias->str,
              (int) m_distinct, (int) m_save_sum_fields,
              (ulong) m_rows_limit, MY_TEST(m_group)));

  if (use_temp_pool && !(test_flags & TEST_KEEP_TMP_TABLES))
    m_temp_pool_slot = bitmap_lock_set_next(&temp_pool);

  if (m_temp_pool_slot != MY_BIT_NONE) // we got a slot
    sprintf(path, "%s-%lx-%i", tmp_file_prefix,
            current_pid, m_temp_pool_slot);
  else
  {
    /* if we run out of slots or we are not using tempool */
    sprintf(path, "%s-%lx-%lx-%x", tmp_file_prefix,current_pid,
            (ulong) thd->thread_id, thd->tmp_table++);
  }

  /*
    No need to change table name to lower case as we are only creating
    MyISAM, Aria or HEAP tables here
  */
  fn_format(path, path, mysql_tmpdir, "",
            MY_REPLACE_EXT|MY_UNPACK_FILENAME);

  if (m_group)
  {
    ORDER **prev= &m_group;
    if (!param->quick_group)
      m_group= 0;                               // Can't use group key
    else for (ORDER *tmp= m_group ; tmp ; tmp= tmp->next)
    {
      /* Exclude found constant from the list */
      if ((*tmp->item)->const_item())
      {
        *prev= tmp->next;
        param->group_parts--;
        continue;
      }
      else
        prev= &(tmp->next);
      /*
        marker == 4 means two things:
        - store NULLs in the key, and
        - convert BIT fields to 64-bit long, needed because MEMORY tables
          can't index BIT fields.
      */
      (*tmp->item)->marker=4;			// Store null in key
      if ((*tmp->item)->too_big_for_varchar())
        m_using_unique_constraint= true;
    }
    if (param->group_length >= MAX_BLOB_WIDTH)
      m_using_unique_constraint= true;
    if (m_group)
      m_distinct= 0;                           // Can't use distinct
  }

  m_alloced_field_count= param->field_count+param->func_count+param->sum_func_count;
  DBUG_ASSERT(m_alloced_field_count);
  const uint field_count= m_alloced_field_count;

  /*
    When loose index scan is employed as access method, it already
    computes all groups and the result of all aggregate functions. We
    make space for the items of the aggregate function in the list of
    functions TMP_TABLE_PARAM::items_to_copy, so that the values of
    these items are stored in the temporary table.
  */
  if (param->precomputed_group_by)
    copy_func_count+= param->sum_func_count;
  
  init_sql_alloc(&own_root, "tmp_table", TABLE_ALLOC_BLOCK_SIZE, 0,
                 MYF(MY_THREAD_SPECIFIC));

  if (!multi_alloc_root(&own_root,
                        &table, sizeof(*table),
                        &share, sizeof(*share),
                        &reg_field, sizeof(Field*) * (field_count+1),
                        &m_default_field, sizeof(Field*) * (field_count),
                        &blob_field, sizeof(uint)*(field_count+1),
                        &m_from_field, sizeof(Field*)*field_count,
                        &param->items_to_copy,
                          sizeof(param->items_to_copy[0])*(copy_func_count+1),
                        &param->keyinfo, sizeof(*param->keyinfo),
                        &m_key_part_info,
                        sizeof(*m_key_part_info)*(param->group_parts+1),
                        &param->start_recinfo,
                        sizeof(*param->recinfo)*(field_count*2+4),
                        &tmpname, (uint) strlen(path)+1,
                        &m_group_buff, (m_group && ! m_using_unique_constraint ?
                                      param->group_length : 0),
                        &m_bitmaps, bitmap_buffer_size(field_count)*6,
                        NullS))
  {
    DBUG_RETURN(NULL);				/* purecov: inspected */
  }
  /* Copy_field belongs to TMP_TABLE_PARAM, allocate it in THD mem_root */
  if (!(param->copy_field= new (thd->mem_root) Copy_field[field_count]))
  {
    free_root(&own_root, MYF(0));               /* purecov: inspected */
    DBUG_RETURN(NULL);				/* purecov: inspected */
  }
  strmov(tmpname, path);
  /* make table according to fields */

  bzero((char*) table,sizeof(*table));
  bzero((char*) reg_field, sizeof(Field*) * (field_count+1));
  bzero((char*) m_default_field, sizeof(Field*) * (field_count));
  bzero((char*) m_from_field, sizeof(Field*) * field_count);

  table->mem_root= own_root;
  mem_root_save= thd->mem_root;
  thd->mem_root= &table->mem_root;

  table->field=reg_field;
  table->alias.set(table_alias->str, table_alias->length, table_alias_charset);

  table->reginfo.lock_type=TL_WRITE;	/* Will be updated */
  table->map=1;
  table->temp_pool_slot= m_temp_pool_slot;
  table->copy_blobs= 1;
  table->in_use= thd;
  table->no_rows_with_nulls= param->force_not_null_cols;
  table->update_handler= NULL;
  table->check_unique_buf= NULL;

  table->s= share;
  init_tmp_table_share(thd, share, "", 0, "(temporary)", tmpname);
  share->blob_field= blob_field;
  share->table_charset= param->table_charset;
  share->primary_key= MAX_KEY;               // Indicate no primary key
  if (param->schema_table)
    share->db= INFORMATION_SCHEMA_NAME;

  param->using_outer_summary_function= 0;
  thd->mem_root= mem_root_save;
  DBUG_RETURN(table);
}


bool Create_json_table::finalize(THD *thd,
                                TABLE *table,
                                TMP_TABLE_PARAM *param,
                                bool do_not_open, bool keep_row_order)
{
  DBUG_ENTER("Create_json_table::finalize");
  DBUG_ASSERT(table);

  uint hidden_null_pack_length;
  uint null_pack_length;
  bool  use_packed_rows= false;
  uchar *pos;
  uchar *null_flags;
  //KEY *keyinfo;
  TMP_ENGINE_COLUMNDEF *recinfo;
  TABLE_SHARE  *share= table->s;
  Copy_field *copy= param->copy_field;

  MEM_ROOT *mem_root_save= thd->mem_root;
  thd->mem_root= &table->mem_root;

  DBUG_ASSERT(m_alloced_field_count >= share->fields);
  DBUG_ASSERT(m_alloced_field_count >= share->blob_fields);

  /* If result table is small; use a heap */
  /* future: storage engine selection can be made dynamic? */
  if (share->blob_fields || m_using_unique_constraint
      || (thd->variables.big_tables && !(m_select_options & SELECT_SMALL_RESULT))
      || (m_select_options & TMP_TABLE_FORCE_MYISAM)
      || thd->variables.tmp_memory_table_size == 0)
  {
    share->db_plugin= ha_lock_engine(0, TMP_ENGINE_HTON);
    table->file= get_new_handler(share, &table->mem_root,
                                 share->db_type());
    if (m_group &&
	(param->group_parts > table->file->max_key_parts() ||
	 param->group_length > table->file->max_key_length()))
      m_using_unique_constraint= true;
  }
  else
  {
    share->db_plugin= ha_lock_engine(0, heap_hton);
    table->file= get_new_handler(share, &table->mem_root,
                                 share->db_type());
  }
  if (!table->file)
    goto err;

  if (table->file->set_ha_share_ref(&share->ha_share))
  {
    delete table->file;
    goto err;
  }

  if (!m_using_unique_constraint)
    share->reclength+= m_group_null_items; // null flag is stored separately

  if (share->blob_fields == 0)
  {
    /* We need to ensure that first byte is not 0 for the delete link */
    if (param->hidden_field_count)
      m_hidden_null_count++;
    else
      m_null_count++;
  }
  hidden_null_pack_length= (m_hidden_null_count + 7 +
                            m_hidden_uneven_bit_length) / 8;
  null_pack_length= (hidden_null_pack_length +
                     (m_null_count + m_uneven_bit_length + 7) / 8);
  share->reclength+= null_pack_length;
  if (!share->reclength)
    share->reclength= 1;                // Dummy select

#ifdef MDEV17933
  /* Use packed rows if there is blobs or a lot of space to gain */
  if (share->blob_fields ||
      (string_total_length() >= STRING_TOTAL_LENGTH_TO_PACK_ROWS &&
       (share->reclength / string_total_length() <= RATIO_TO_PACK_ROWS ||
        string_total_length() / string_count() >= AVG_STRING_LENGTH_TO_PACK_ROWS)))
    use_packed_rows= 1;

  {
    uint alloc_length= ALIGN_SIZE(share->reclength + MI_UNIQUE_HASH_LENGTH+1);
    share->rec_buff_length= alloc_length;
    if (!(table->record[0]= (uchar*)
                            alloc_root(&table->mem_root, alloc_length*3)))
      goto err;
    table->record[1]= table->record[0]+alloc_length;
    share->default_values= table->record[1]+alloc_length;
  }
#endif /*MDEV17933*/

  setup_tmp_table_column_bitmaps(table, m_bitmaps);

  recinfo=param->start_recinfo;
  null_flags=(uchar*) table->record[0];
  pos=table->record[0]+ null_pack_length;
  if (null_pack_length)
  {
    bzero((uchar*) recinfo,sizeof(*recinfo));
    recinfo->type=FIELD_NORMAL;
    recinfo->length=null_pack_length;
    recinfo++;
    bfill(null_flags,null_pack_length,255);	// Set null fields

    table->null_flags= (uchar*) table->record[0];
    share->null_fields= m_null_count + m_hidden_null_count;
    share->null_bytes= share->null_bytes_for_compare= null_pack_length;
  }
  m_null_count= (share->blob_fields == 0) ? 1 : 0;
  m_hidden_field_count= param->hidden_field_count;
  for (uint i= 0; i < share->fields; i++, recinfo++)
  {
    Field *field= table->field[i];
    uint length;
    bzero((uchar*) recinfo,sizeof(*recinfo));

    if (!(field->flags & NOT_NULL_FLAG))
    {
      recinfo->null_bit= (uint8)1 << (m_null_count & 7);
      recinfo->null_pos= m_null_count/8;
      field->move_field(pos, null_flags + m_null_count/8,
			(uint8)1 << (m_null_count & 7));
      m_null_count++;
    }
    else
      field->move_field(pos,(uchar*) 0,0);
    if (field->type() == MYSQL_TYPE_BIT)
    {
      /* We have to reserve place for extra bits among null bits */
      ((Field_bit*) field)->set_bit_ptr(null_flags + m_null_count / 8,
                                        m_null_count & 7);
      m_null_count+= (field->field_length & 7);
    }
    field->reset();

    /*
      Test if there is a default field value. The test for ->ptr is to skip
      'offset' fields generated by initialize_tables
    */
    if (m_default_field[i] && m_default_field[i]->ptr)
    {
      /* 
         default_field[i] is set only in the cases  when 'field' can
         inherit the default value that is defined for the field referred
         by the Item_field object from which 'field' has been created.
      */
      const Field *orig_field= m_default_field[i];
      /* Get the value from default_values */
      if (orig_field->is_null_in_record(orig_field->table->s->default_values))
        field->set_null();
      else
      {
        field->set_notnull();
        memcpy(field->ptr,
               orig_field->ptr_in_record(orig_field->table->s->default_values),
               field->pack_length_in_rec());
      }
    } 

    if (m_from_field[i])
    {						/* Not a table Item */
      copy->set(field, m_from_field[i], m_save_sum_fields);
      copy++;
    }
    length=field->pack_length();
    pos+= length;

    /* Make entry for create table */
    recinfo->length=length;
    recinfo->type= field->tmp_engine_column_type(use_packed_rows);
    if (!--m_hidden_field_count)
      m_null_count= (m_null_count + 7) & ~7;    // move to next byte

    // fix table name in field entry
    field->set_table_name(&table->alias);
  }

  param->copy_field_end= copy;
  param->recinfo= recinfo;              	// Pointer to after last field
  store_record(table,s->default_values);        // Make empty default record

  if (thd->variables.tmp_memory_table_size == ~ (ulonglong) 0)	// No limit
    share->max_rows= ~(ha_rows) 0;
  else
    share->max_rows= (ha_rows) (((share->db_type() == heap_hton) ?
                                 MY_MIN(thd->variables.tmp_memory_table_size,
                                     thd->variables.max_heap_table_size) :
                                 thd->variables.tmp_disk_table_size) /
                                share->reclength);
  set_if_bigger(share->max_rows,1);		// For dummy start options
  /*
    Push the LIMIT clause to the temporary table creation, so that we
    materialize only up to 'rows_limit' records instead of all result records.
  */
  set_if_smaller(share->max_rows, m_rows_limit);
  param->end_write_records= m_rows_limit;

  //keyinfo= param->keyinfo;

#ifdef MDEV17933
  if (m_group)
  {
    DBUG_PRINT("info",("Creating group key in temporary table"));
    table->group= m_group;				/* Table is grouped by key */
    param->group_buff= m_group_buff;
    share->keys=1;
    share->uniques= MY_TEST(m_using_unique_constraint);
    table->key_info= table->s->key_info= keyinfo;
    table->keys_in_use_for_query.set_bit(0);
    share->keys_in_use.set_bit(0);
    keyinfo->key_part= m_key_part_info;
    keyinfo->flags=HA_NOSAME | HA_BINARY_PACK_KEY | HA_PACK_KEY;
    keyinfo->ext_key_flags= keyinfo->flags;
    keyinfo->usable_key_parts=keyinfo->user_defined_key_parts= param->group_parts;
    keyinfo->ext_key_parts= keyinfo->user_defined_key_parts;
    keyinfo->key_length=0;
    keyinfo->rec_per_key=NULL;
    keyinfo->read_stats= NULL;
    keyinfo->collected_stats= NULL;
    keyinfo->algorithm= HA_KEY_ALG_UNDEF;
    keyinfo->is_statistics_from_stat_tables= FALSE;
    keyinfo->name= group_key;
    ORDER *cur_group= m_group;
    for (; cur_group ; cur_group= cur_group->next, m_key_part_info++)
    {
      Field *field=(*cur_group->item)->get_tmp_table_field();
      DBUG_ASSERT(field->table == table);
      bool maybe_null=(*cur_group->item)->maybe_null;
      m_key_part_info->null_bit=0;
      m_key_part_info->field=  field;
      m_key_part_info->fieldnr= field->field_index + 1;
      if (cur_group == m_group)
        field->key_start.set_bit(0);
      m_key_part_info->offset= field->offset(table->record[0]);
      m_key_part_info->length= (uint16) field->key_length();
      m_key_part_info->type=   (uint8) field->key_type();
      m_key_part_info->key_type =
	((ha_base_keytype) m_key_part_info->type == HA_KEYTYPE_TEXT ||
	 (ha_base_keytype) m_key_part_info->type == HA_KEYTYPE_VARTEXT1 ||
	 (ha_base_keytype) m_key_part_info->type == HA_KEYTYPE_VARTEXT2) ?
	0 : FIELDFLAG_BINARY;
      m_key_part_info->key_part_flag= 0;
      if (!m_using_unique_constraint)
      {
        cur_group->buff=(char*) m_group_buff;

        if (maybe_null && !field->null_bit)
        {
          /*
            This can only happen in the unusual case where an outer join
            table was found to be not-nullable by the optimizer and we
            the item can't really be null.
            We solve this by marking the item as !maybe_null to ensure
            that the key,field and item definition match.
          */
          (*cur_group->item)->maybe_null= maybe_null= 0;
        }

	if (!(cur_group->field= field->new_key_field(thd->mem_root,table,
                                                     m_group_buff +
                                                     MY_TEST(maybe_null),
                                                     m_key_part_info->length,
                                                     field->null_ptr,
                                                     field->null_bit)))
	  goto err; /* purecov: inspected */

	if (maybe_null)
	{
	  /*
	    To be able to group on NULL, we reserved place in group_buff
	    for the NULL flag just before the column. (see above).
	    The field data is after this flag.
	    The NULL flag is updated in 'end_update()' and 'end_write()'
	  */
	  keyinfo->flags|= HA_NULL_ARE_EQUAL;	// def. that NULL == NULL
	  m_key_part_info->null_bit=field->null_bit;
	  m_key_part_info->null_offset= (uint) (field->null_ptr -
					      (uchar*) table->record[0]);
          cur_group->buff++;                        // Pointer to field data
	  m_group_buff++;                         // Skipp null flag
	}
        m_group_buff+= cur_group->field->pack_length();
      }
      keyinfo->key_length+=  m_key_part_info->length;
    }
    /*
      Ensure we didn't overrun the group buffer. The < is only true when
      some maybe_null fields was changed to be not null fields.
    */
    DBUG_ASSERT(m_using_unique_constraint ||
                m_group_buff <= param->group_buff + param->group_length);
  }

  if (m_distinct && share->fields != param->hidden_field_count)
  {
    uint i;
    Field **reg_field;
    /*
      Create an unique key or an unique constraint over all columns
      that should be in the result.  In the temporary table, there are
      'param->hidden_field_count' extra columns, whose null bits are stored
      in the first 'hidden_null_pack_length' bytes of the row.
    */
    DBUG_PRINT("info",("hidden_field_count: %d", param->hidden_field_count));

    if (share->blob_fields)
    {
      /*
        Special mode for index creation in MyISAM used to support unique
        indexes on blobs with arbitrary length. Such indexes cannot be
        used for lookups.
      */
      share->uniques= 1;
    }
    null_pack_length-=hidden_null_pack_length;
    keyinfo->user_defined_key_parts=
      ((share->fields - param->hidden_field_count)+
       (share->uniques ? MY_TEST(null_pack_length) : 0));
    keyinfo->ext_key_parts= keyinfo->user_defined_key_parts;
    keyinfo->usable_key_parts= keyinfo->user_defined_key_parts;
    table->distinct= 1;
    share->keys= 1;
    if (!(m_key_part_info= (KEY_PART_INFO*)
          alloc_root(&table->mem_root,
                     keyinfo->user_defined_key_parts * sizeof(KEY_PART_INFO))))
      goto err;
    bzero((void*) m_key_part_info, keyinfo->user_defined_key_parts * sizeof(KEY_PART_INFO));
    table->keys_in_use_for_query.set_bit(0);
    share->keys_in_use.set_bit(0);
    table->key_info= table->s->key_info= keyinfo;
    keyinfo->key_part= m_key_part_info;
    keyinfo->flags=HA_NOSAME | HA_NULL_ARE_EQUAL | HA_BINARY_PACK_KEY | HA_PACK_KEY;
    keyinfo->ext_key_flags= keyinfo->flags;
    keyinfo->key_length= 0;  // Will compute the sum of the parts below.
    keyinfo->name= distinct_key;
    keyinfo->algorithm= HA_KEY_ALG_UNDEF;
    keyinfo->is_statistics_from_stat_tables= FALSE;
    keyinfo->read_stats= NULL;
    keyinfo->collected_stats= NULL;

    /*
      Needed by non-merged semi-joins: SJ-Materialized table must have a valid 
      rec_per_key array, because it participates in join optimization. Since
      the table has no data, the only statistics we can provide is "unknown",
      i.e. zero values.

      (For table record count, we calculate and set JOIN_TAB::found_records,
       see get_delayed_table_estimates()).
    */
    size_t rpk_size= keyinfo->user_defined_key_parts * sizeof(keyinfo->rec_per_key[0]);
    if (!(keyinfo->rec_per_key= (ulong*) alloc_root(&table->mem_root, 
                                                    rpk_size)))
      goto err;
    bzero(keyinfo->rec_per_key, rpk_size);

    /*
      Create an extra field to hold NULL bits so that unique indexes on
      blobs can distinguish NULL from 0. This extra field is not needed
      when we do not use UNIQUE indexes for blobs.
    */
    if (null_pack_length && share->uniques)
    {
      m_key_part_info->null_bit=0;
      m_key_part_info->offset=hidden_null_pack_length;
      m_key_part_info->length=null_pack_length;
      m_key_part_info->field= new Field_string(table->record[0],
                                             (uint32) m_key_part_info->length,
                                             (uchar*) 0,
                                             (uint) 0,
                                             Field::NONE,
                                             &null_clex_str, &my_charset_bin);
      if (!m_key_part_info->field)
        goto err;
      m_key_part_info->field->init(table);
      m_key_part_info->key_type=FIELDFLAG_BINARY;
      m_key_part_info->type=    HA_KEYTYPE_BINARY;
      m_key_part_info->fieldnr= m_key_part_info->field->field_index + 1;
      m_key_part_info++;
    }
    /* Create a distinct key over the columns we are going to return */
    for (i= param->hidden_field_count, reg_field= table->field + i ;
         i < share->fields;
         i++, reg_field++, m_key_part_info++)
    {
      m_key_part_info->field= *reg_field;
      (*reg_field)->flags |= PART_KEY_FLAG;
      if (m_key_part_info == keyinfo->key_part)
        (*reg_field)->key_start.set_bit(0);
      m_key_part_info->null_bit= (*reg_field)->null_bit;
      m_key_part_info->null_offset= (uint) ((*reg_field)->null_ptr -
                                          (uchar*) table->record[0]);

      m_key_part_info->offset=   (*reg_field)->offset(table->record[0]);
      m_key_part_info->length=   (uint16) (*reg_field)->pack_length();
      m_key_part_info->fieldnr= (*reg_field)->field_index + 1;
      /* TODO:
        The below method of computing the key format length of the
        key part is a copy/paste from opt_range.cc, and table.cc.
        This should be factored out, e.g. as a method of Field.
        In addition it is not clear if any of the Field::*_length
        methods is supposed to compute the same length. If so, it
        might be reused.
      */
      m_key_part_info->store_length= m_key_part_info->length;

      if ((*reg_field)->real_maybe_null())
      {
        m_key_part_info->store_length+= HA_KEY_NULL_LENGTH;
        m_key_part_info->key_part_flag |= HA_NULL_PART;
      }
      m_key_part_info->key_part_flag|= (*reg_field)->key_part_flag();
      m_key_part_info->store_length+= (*reg_field)->key_part_length_bytes();
      keyinfo->key_length+= m_key_part_info->store_length;

      m_key_part_info->type=     (uint8) (*reg_field)->key_type();
      m_key_part_info->key_type =
	((ha_base_keytype) m_key_part_info->type == HA_KEYTYPE_TEXT ||
	 (ha_base_keytype) m_key_part_info->type == HA_KEYTYPE_VARTEXT1 ||
	 (ha_base_keytype) m_key_part_info->type == HA_KEYTYPE_VARTEXT2) ?
	0 : FIELDFLAG_BINARY;
    }
  }
#endif /*MDEV17933*/

  if (unlikely(thd->is_fatal_error))             // If end of memory
    goto err;					 /* purecov: inspected */
  share->db_record_offset= 1;
  table->used_for_duplicate_elimination= (param->sum_func_count == 0 &&
                                          (table->group || table->distinct));
  table->keep_row_order= keep_row_order;

  if (open_tmp_table(table))
    goto err;

  // Make empty record so random data is not written to disk
  empty_record(table);

  thd->mem_root= mem_root_save;

  DBUG_RETURN(false);

err:
  thd->mem_root= mem_root_save;
  DBUG_RETURN(true);                            /* purecov: inspected */
}


bool Create_json_table::add_schema_fields(THD *thd, TABLE *table,
                                         TMP_TABLE_PARAM *param,
                                         const ST_SCHEMA_TABLE &schema_table,
                                         const MY_BITMAP &bitmap)
{
  DBUG_ENTER("Create_json_table::add_schema_fields");
  DBUG_ASSERT(table);
  DBUG_ASSERT(table->field);
  DBUG_ASSERT(table->s->blob_field);
  DBUG_ASSERT(table->s->reclength == 0);
  DBUG_ASSERT(table->s->fields == 0);
  DBUG_ASSERT(table->s->blob_fields == 0);

  TABLE_SHARE *share= table->s;
  ST_FIELD_INFO *defs= schema_table.fields_info;
  uint fieldnr;
  MEM_ROOT *mem_root_save= thd->mem_root;
  thd->mem_root= &table->mem_root;

  for (fieldnr= 0; !defs[fieldnr].end_marker(); fieldnr++)
  {
    const ST_FIELD_INFO &def= defs[fieldnr];
    bool visible= bitmap_is_set(&bitmap, fieldnr);
    Record_addr addr(def.nullable());
    const Type_handler *h= def.type_handler();
    Field *field= h->make_schema_field(&table->mem_root, table,
                                       addr, def, visible);
    if (!field)
    {
      thd->mem_root= mem_root_save;
      DBUG_RETURN(true); // EOM
    }
    field->init(table);
    switch (def.def()) {
    case DEFAULT_NONE:
      field->flags|= NO_DEFAULT_VALUE_FLAG;
      break;
    case DEFAULT_TYPE_IMPLICIT:
      break;
    default:
      DBUG_ASSERT(0);
      break;
    }
    add_field(table, field, fieldnr, param->force_not_null_cols);
  }

  share->fields= fieldnr;
  share->blob_fields= m_blob_count;
  table->field[fieldnr]= 0;                     // End marker
  share->blob_field[m_blob_count]= 0;           // End marker
  param->func_count= 0;
  share->column_bitmap_size= bitmap_buffer_size(share->fields);

  thd->mem_root= mem_root_save;
  DBUG_RETURN(false);
}


void Create_json_table::cleanup_on_failure(THD *thd, TABLE *table)
{
  if (table)
    free_tmp_table(thd, table);
  if (m_temp_pool_slot != MY_BIT_NONE)
    bitmap_lock_clear_bit(&temp_pool, m_temp_pool_slot);
}


TABLE *create_table_for_function(THD *thd, TABLE_LIST *sql_table)
{
  my_bitmap_map *buf;
  MY_BITMAP bitmap;
  TMP_TABLE_PARAM tp;
  TABLE *table;
  uint field_count=4;
  
  DBUG_ENTER("create_table_for_function");

  if (!(buf= (my_bitmap_map*) thd->alloc(bitmap_buffer_size(4))))
    return NULL;
  my_bitmap_init(&bitmap, buf, 4, 0); 

  tp.init();
  tp.table_charset= system_charset_info;
  tp.field_count= 4;
  {
    Create_json_table maker(&tp, (ORDER *) NULL, false, false,
                           TMP_TABLE_ALL_COLUMNS, HA_POS_ERROR);
    ST_SCHEMA_TABLE *schema_table;
    LEX_CSTRING tn;
    tn.str= "CHARACTER_SETS";
    tn.length= 14;
    schema_table= sql_table->schema_table= find_schema_table(thd, &tn); 
    sql_table->schema_table_name= tn;

    if (!(table= maker.start(thd, &tp, &sql_table->alias)) ||
        maker.add_schema_fields(thd, table, &tp, *schema_table, bitmap) || 
        /*add_json_table_fields(thd, &maker, sql_table->json_table) ||*/
        maker.finalize(thd, table, &tp, false, false))
    {
      maker.cleanup_on_failure(thd, table);
      return NULL;
    }
  }
  my_bitmap_map* bitmaps=
    (my_bitmap_map*) thd->alloc(bitmap_buffer_size(field_count));
  my_bitmap_init(&table->def_read_set, (my_bitmap_map*) bitmaps, field_count,
                 FALSE);
  table->read_set= &table->def_read_set;
  bitmap_clear_all(table->read_set);
  table->alias_name_used= true;
  table->next= thd->derived_tables;
  thd->derived_tables= table;
  table->s->tmp_table= SYSTEM_TMP_TABLE;
  table->grant.privilege= SELECT_ACL;

  sql_table->table= table;
  sql_table->schema_table->fill_table(thd, sql_table, NULL);

  DBUG_RETURN(table);
}


#ifdef MDEV_17399
TABLE *create_table_for_function(THD *thd, TABLE_LIST *sql_table)
{
  MEM_ROOT own_root, *mem_root_save;
  TABLE *table;
  Table_function_json_table *def= sql_table->table_function;
  uint field_count;
  ulong data_offset;
  List_iterator_fast col_t(def->m_columns);
  handler *h;
  const Json_table_colunn *c;

  DBUG_ENTER("create_table_for_function");

  /*
  for (field_count= 0, fields= fields_info; !fields->end_marker(); fields++)
    field_count++;
  if (!(buf= (my_bitmap_map*) thd->alloc(bitmap_buffer_size(field_count))))
    DBUG_RETURN(NULL);
  my_bitmap_init(&bitmap, buf, field_count, 0);

  if (!thd->stmt_arena->is_conventional() &&
      thd->mem_root != thd->stmt_arena->mem_root)
    all_items= thd->stmt_arena->free_list;
  else
    all_items= thd->free_list;

  mark_all_fields_used_in_query(thd, fields_info, &bitmap, all_items);
  */

  field_count= def->m_columns.elements;
  if (field_count > MAX_FIELDS)
  {
    my_message(ER_TOO_MANY_FIELDS, ER_THD(thd, ER_TOO_MANY_FIELDS), MYF(0));
    DBUG_RETURN(0);
  }
  init_sql_alloc(&own_root, "tmp_table", TABLE_ALLOC_BLOCK_SIZE, 0,
                 MYF(MY_THREAD_SPECIFIC));

  if (!multi_alloc_root(&own_root, &table, sizeof(*table),
                        &reg_field, sizeof(Field*) * (field_count+1),
        /*
                        &share, sizeof(*share),
                        &m_default_field, sizeof(Field*) * (field_count),
                        &blob_field, sizeof(uint)*(field_count+1),
                        &m_from_field, sizeof(Field*)*field_count,
                        &param->items_to_copy,
                          sizeof(param->items_to_copy[0])*(copy_func_count+1),
                        &param->keyinfo, sizeof(*param->keyinfo),
                        &m_key_part_info,
                        sizeof(*m_key_part_info)*(param->group_parts+1),
                        &param->start_recinfo,
                        sizeof(*param->recinfo)*(field_count*2+4),
                        &tmpname, (uint) strlen(path)+1,
                        &m_group_buff, (m_group && ! m_using_unique_constraint ?
                                      param->group_length : 0),
                        &m_bitmaps, bitmap_buffer_size(field_count)*6,
                        */
                        NullS))
  {
    DBUG_RETURN(NULL);				/* purecov: inspected */
  }
  bzero((char*) table,sizeof(*table));
  bzero((char*) reg_field, sizeof(Field*) * (field_count+1));

  table->mem_root= own_root;
  /*
  mem_root_save= thd->mem_root;
  thd->mem_root= &table->mem_root;
  */

  table->field=reg_field;
  table->alias.set(table_alias->str, table_alias->length, table_alias_charset);

  table->reginfo.lock_type=TL_READ;
  table->map=1;
  table->temp_pool_slot= m_temp_pool_slot;
  table->copy_blobs= 1;
  table->in_use= thd;
  table->no_rows_with_nulls= FALSE;
  table->update_handler= NULL;
  table->check_unique_buf= NULL;

   table->s= share;
   init_tmp_table_share(thd, &share, db, 0, table_name, path); 

  /* add fields */
  mem_root_save= thd->mem_root;
  thd->mem_root= &table->mem_root;

  h= new(thd->mem_root) ha_table_function();
  h->init();

  while ((c= col_it++))
  {
    if (c->prepare_stage1(thd, thd->mem_root, h, h->ha_table_flags()))
      DBUG_RETURN(0);
    if (check_column(name(c->field_name.str)))
    {
      my_error(ER_WRONG_COLUMN_NAME, MYF(0), sql_field->field_name.str);
      DBUG_RETURN(0);
    }
    /* check duplicates. */
  }

  record_offset= 0;
  col_it.rewind();
  while ((c= col_it++))
  {
    if (sql_field->prepare_stage2(file, file->ha_table_flags()))
      DBUG_RETURN(0);
    sql_field->offset= record_offset;
    record_offset+= sql_field->pack_length;
    totlength+= sql_field->length;
    n_length+= sql_field->field_name_length + 1;
  }
  reclength= record_offset + data_offset;

  data_offset= (field_count + 7) / 8; /* all fields can be NULL. */

  while (c= col_it++)
  {
    Record_addr addr(buff + field->offset + data_offset,
        null_pos + null_count / 8, null_count & 7);
    Column_definition_attributes cda(*c->m_field);
    Field *f= cda.make_field(*table->s, table->mem_root, &addr,
        c->type_handler(), &c->field_name, c->flags);

    f->init(table);
    if (!(c->flags & NOT_NULL_FLAG))
      *f->null_ptr!= f->null_bit;
    /* error= make_empty_rec_store_default(thd, regfield, field->default_value); */



  }
  DBUG_RETURN(0);
}
#endif /*MDEV_17399*/


