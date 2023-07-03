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
        // public static BinaryExportor Ins { get; } = new BinaryExportor();    // ����״̬�ˣ����ܼ���ʹ�þ�̬����
        protected int _curRecordIndex = 0;
        protected Dictionary<int, Dictionary<DType, int>> _cache = new();
        protected int _nestBean = 0;            // ֻ�Ե�һ��Bean����table row��ȥ�أ�Ƕ�׵�Bean�����

        public void WriteList(DefTable table, List<Record> datas, ByteBuf x)
        {
            // ���л����ű��ڴ˴�����Hash Cache
            // ������Type = {Luban.Job.Common.Types.TList}��������
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
            // �˴���һ����¼������field�������л�
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
                        valueIndexMap = new Dictionary<DType, int>(20); // TODO: ���ݱ��ȷ���
                        _cache.Add(curIndex, valueIndexMap);
                    }

                    if (valueIndexMap.TryGetValue(field, out var preIndex))
                    {
                        // �ҵ�����ͬ�У�ֱ��д�����õ��±꣬����д������field
                        // TODO: �����4byte + 1byte��ʱ���ֱ��д���ݻ���
                        // TODO: ����ǿյ����飬�ǲ���Ҳ������
                        // x.WriteSint(-preIndex-1);       // ����preIndex = 0ʱ�������ߣ�������-1 offset
                        x.WriteSize(-preIndex - 1);
                        continue;
                    } else
                    {
                        valueIndexMap.Add(field, _curRecordIndex);
                    }

                    // TODO: add hash to cache, field index = {index-1}
                    // TODO: Ҳ���Դ˴����غ�ֱ��д�븺��
                    // TODO: ע��nullable case�Ĵ���
                    // TODO: ��û�п��ܴ��ڵ���Ƕ�׵��߼���
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
            // TODO: �ӻ������鿴�Ƿ���Hashƥ��ļ�¼���Ѿ������ϲ���ɣ�
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
