using Luban.Job.Cfg.Defs;
using Luban.Job.Common.Defs;
using System;
using System.Collections.Generic;

namespace Luban.Job.Cfg.Generate
{
    class EditorCppRender : CodeRenderBase
    {
        public override string Render(DefConst c)
        {
            return "// const";
        }

        public override string Render(DefEnum e)
        {
            return "// enum";
        }

        public override string Render(DefBean b)
        {
            return "// bean";
        }

        public override string Render(DefTable p)
        {
            return "// table";
        }

        public override string RenderService(string name, string module, List<DefTable> tables)
        {
            return "// service";
        }
    }
}
