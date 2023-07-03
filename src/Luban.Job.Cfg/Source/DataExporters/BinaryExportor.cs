using Bright.Serialization;
using Luban.Job.Cfg.Datas;
using Luban.Job.Cfg.DataSources;
using Luban.Job.Cfg.DataVisitors;
using Luban.Job.Cfg.Defs;
using System.Collections.Generic;

namespace Luban.Job.Cfg.DataExporters
{
    class BinaryExportor : IDataActionVisitor<ByteBuf>
    {
        // public static BinaryExportor Ins { get; } = new BinaryExportor();    // 保存状态了，不能继续使用静态单例
        protected int _curRecordIndex = 0;
        protected Dictionary<int, Dictionary<DType, int>> _cache = new();
        protected int _nestBean = 0;            // 只对第一层Bean（即table row）去重，嵌套的Bean不理会

        public void WriteList(DefTable table, List<Record> datas, ByteBuf x)
        {
            // 序列化单张表，在此处清理Hash Cache
            // 对类型Type = {Luban.Job.Common.Types.TList}建立缓存
            x.WriteSize(datas.Count);
            _curRecordIndex = 0;
            _cache.Clear();
            foreach (var d in datas)
            {
                d.Data.Apply(this, x);
                ++ _curRecordIndex;
            }
        }

        public void Accept(DBool type, ByteBuf x)
        {
            x.WriteBool(type.Value);
        }

        public void Accept(DByte type, ByteBuf x)
        {
            x.WriteByte(type.Value);
        }

        public void Accept(DShort type, ByteBuf x)
        {
            x.WriteShort(type.Value);
        }

        public void Accept(DFshort type, ByteBuf x)
        {
            x.WriteFshort(type.Value);
        }

        public void Accept(DInt type, ByteBuf x)
        {
            x.WriteInt(type.Value);
        }

        public void Accept(DFint type, ByteBuf x)
        {
            x.WriteFint(type.Value);
        }

        public void Accept(DLong type, ByteBuf x)
        {
            x.WriteLong(type.Value);
        }

        public void Accept(DFlong type, ByteBuf x)
        {
            x.WriteFlong(type.Value);
        }

        public void Accept(DFloat type, ByteBuf x)
        {
            x.WriteFloat(type.Value);
        }

        public void Accept(DDouble type, ByteBuf x)
        {
            x.WriteDouble(type.Value);
        }

        public void Accept(DEnum type, ByteBuf x)
        {
            x.WriteInt(type.Value);
        }

        public void Accept(DString type, ByteBuf x)
        {
            x.WriteString(type.Value);
        }

        public void Accept(DBytes type, ByteBuf x)
        {
            x.WriteBytes(type.Value);
        }

        public void Accept(DText type, ByteBuf x)
        {
            x.WriteString(type.Key);
            x.WriteString(type.TextOfCurrentAssembly);
        }

        public void Accept(DBean type, ByteBuf x)
        {
            ++ _nestBean;
            // 此处对一条记录的所有field进行序列化
            var bean = type.Type;
            if (bean.IsAbstractType)
            {
                x.WriteInt(type.ImplType.Id);
            }

            var defFields = type.ImplType.HierarchyFields;
            int index = 0;
            foreach (var field in type.Fields)
            {
                int curIndex = index;
                var defField = (DefField)defFields[index++];
                if (!defField.NeedExport)
                {
                    continue;
                }
                if (defField.CType.IsNullable)
                {
                    if (field == null)
                    {
                        x.WriteBool(false);
                        continue;
                    }

                    x.WriteBool(true);
                }

                if (_nestBean == 1 && defField.CType.TypeName == "list" && field.ToString() != "[]")
                {
                    if (!_cache.TryGetValue(curIndex, out var valueIndexMap))
                    {
                        valueIndexMap = new Dictionary<DType, int>(20); // TODO: 根据表长度分配
                        _cache.Add(curIndex, valueIndexMap);
                    }

                    if (valueIndexMap.TryGetValue(field, out var preIndex))
                    {
                        // 找到了相同行，直接写入引用的下标，不再写入整个field
                        // TODO: 这里的4byte + 1byte有时候比直接写数据还大
                        // TODO: 如果是空的数组，是不是也会进这里？
                        // x.WriteSint(-preIndex-1);       // 避免preIndex = 0时混淆两者，这里做-1 offset
                        x.WriteSize(-preIndex - 1);
                        continue;
                    } else
                    {
                        valueIndexMap.Add(field, _curRecordIndex);
                    }

                    // TODO: add hash to cache, field index = {index-1}
                    // TODO: 也可以此处判重后直接写入负数
                    // TODO: 注意nullable case的处理
                    // TODO: 有没有可能存在单行嵌套的逻辑？
                }

                field.Apply(this, x);
                //if (defField.CType.IsNullable)
                //{
                //    if (field != null)
                //    {
                //        x.WriteBool(true);
                //        field.Apply(this, x);
                //    }
                //    else
                //    {
                //        x.WriteBool(false);
                //    }
                //}
                //else
                //{
                //    field.Apply(this, x);
                //}
            }

            -- _nestBean;
        }

        public void WriteList(List<DType> datas, ByteBuf x)
        {
            // TODO: 从缓存区查看是否有Hash匹配的记录（已经放在上层完成）
            x.WriteSize(datas.Count);
            // x.WriteSint(datas.Count);
            foreach (var d in datas)
            {
                d.Apply(this, x);
            }
        }

        public void Accept(DArray type, ByteBuf x)
        {
            WriteList(type.Datas, x);
        }

        public void Accept(DList type, ByteBuf x)
        {
            WriteList(type.Datas, x);
        }

        public void Accept(DSet type, ByteBuf x)
        {
            WriteList(type.Datas, x);
        }

        public void Accept(DMap type, ByteBuf x)
        {
            Dictionary<DType, DType> datas = type.Datas;
            x.WriteSize(datas.Count);
            foreach (var e in datas)
            {
                e.Key.Apply(this, x);
                e.Value.Apply(this, x);
            }
        }

        public void Accept(DVector2 type, ByteBuf x)
        {
            x.WriteVector2(type.Value);
        }

        public void Accept(DVector3 type, ByteBuf x)
        {
            x.WriteVector3(type.Value);
        }

        public void Accept(DVector4 type, ByteBuf x)
        {
            x.WriteVector4(type.Value);
        }

        public void Accept(DDateTime type, ByteBuf x)
        {
            x.WriteLong(type.UnixTimeOfCurrentAssembly);
        }
    }
}
