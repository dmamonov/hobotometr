#!/usr/bin/python

import os
import math

col_time = 0
col_read_ops = 1
col_read_err = 2
col_write_ops = 3
col_write_err = 4


class ColumnData:
    def __init__(self, chart, title, data):
        self.chart = chart
        self.title = title
        self.data = data
        self.sum = sum(self.data)
        self.avg = self.sum / len(self.data)
        self.sd = math.sqrt(sum([math.pow(x - self.avg, 2) for x in data]) / len(self.data))
        if self.sum<1.0:
            self.upper_90 = self.avg+self.sd*2
            self.lower_90 = self.avg-self.sd*2
        else:
            self.upper_90 = self.solve_linear(min(data), max(data),
                                              lambda x: sum([min(x, val) for val in self.data]) / self.sum - 0.95)
            self.lower_90 = self.solve_linear(min(data), max(data),
                                              lambda x: sum([max(x, val) for val in self.data]) / self.sum - 0.95)


    def solve_linear(self, a, b, fn):
        c = (a+b)/2.0
        if b-a<1.0:
            return c
        else:
            y = fn(c)
            if y > 0.0:
                return self.solve_linear(a, c, fn)
            else:
                return self.solve_linear(c, b, fn)

    def aggregate(self, group_size):
        assert len(self.data) % group_size == 0
        agg_data = [0.0 for i in range(len(self.data) / group_size)]
        for i in range(len(self.data)):
            agg_data[i / group_size] += self.data[i]
        agg_column = ColumnData(self.chart, self.title + '_agg', [x / group_size for x in agg_data])
        agg_column.sum = self.sum
        agg_column.avg = self.avg
        agg_column.sd = self.sd
        return agg_column


class ChartData:
    def __init__(self, file):
        assert file.endswith('.csv')

        # read meta-data:
        for field in file[:-len('.csv')].split(','):
            [key, value] = field.split('=')
            setattr(self, key, int(value))

        #  read raw data:
        first_line = True
        input_matrix = None
        for line in open(file, 'r'):
            line = line.strip()
            if line <> '':
                items = line.split(',')
                if first_line:
                    input_matrix = [[title.replace("'", '')] for title in items]
                    first_line = False
                else:
                    values = [float(value) for value in items]
                    for i in range(len(values)):
                        input_matrix[i].append(values[i])
        self.columns = [ColumnData(self, input_column[0], input_column[1:]) for input_column in input_matrix]

        self.time_line = self.columns[0]

        # noinspection PyUnresolvedReferences
        self.read_th = self.r_lite + self.r_heavy
        # noinspection PyUnresolvedReferences
        read_title = 'r%d_R%d' % (self.r_lite, self.r_heavy)
        self.read_ops = self.columns[1]
        self.read_ops.title = 'R_' + read_title
        self.read_err = self.columns[2]
        self.read_err.title = 'RE_' + read_title

        # noinspection PyUnresolvedReferences
        self.write_th = self.w_ins + self.w_up_tiny + self.w_up_wide
        # noinspection PyUnresolvedReferences
        write_title = 'i%d_u%d_U%d' % (self.w_ins, self.w_up_tiny, self.w_up_wide)
        self.write_ops = self.columns[3]
        self.write_ops.title = 'W_' + write_title
        self.write_err = self.columns[4]
        self.write_err.title = 'WE_' + write_title

        self.max_th = max(self.read_th, self.write_th)


class ChartView:
    def __init__(self, database, host, title, div_id, data):
        self.database = database
        self.host = host
        self.title = title
        self.div_id = div_id
        self.data = data

    def get_profile(self):
        return self.database + '/' + self.host


class ReportView:
    def __init__(self):
        self.chart_views = []

    def add_chart_view(self, chart_view):
        self.chart_views.append(chart_view)

    def add_report_view(self, report_view):
        self.chart_views.extend(report_view.chart_views)

    def list_titles(self):
        result = list(set([cv.title for cv in self.chart_views]))
        result.sort()
        return result

    def list_profiles(self):
        result = list(set([cv.get_profile() for cv in self.chart_views]))
        result.sort()
        return result

    def get_chart_view(self, profile, title):
        filtered = [cv for cv in self.chart_views if cv.title == title and cv.get_profile() == profile]
        assert len(filtered) <= 1
        if len(filtered) == 1:
            return filtered[0]
        else:
            return None

div_id_sequence = 0


class ReportGenerator:
    def __init__(self, database, host):
        self.database = database
        self.host = host
        self.report_write = ReportView()
        self.report_read = ReportView()

    def color(self, size):
        base = [
            '#3366CC',
            '#DC3912',
            '#FF9900',
            '#109618',
            '#990099',
            '#3B3EAC',
            '#0099C6',
            '#DD4477',
            '#66AA00',
            '#B82E2E',
            '#316395',
            '#994499',
            '#22AA99',
            '#AAAA11',
            '#6633CC',
            '#E67300',
            '#8B0707',
            '#329262',
            '#5574A6',
            '#3B3EAC',
            #my
            '#7401DF',
            '#8A0829',
            '#5F4C0B',
            '#8A2908',
            '#3104B4',
            '#00FFBF',
        ]
        return base[size % len(base)]

    def draw_chart(self, columns, title):
        global div_id_sequence
        div_id_sequence += 1
        div_id = 'chart_%s' % div_id_sequence

        result = ""
        result += """
        function %s() {
            var data = google.visualization.arrayToDataTable([
        """ % div_id
        result += '[%s],\n' % ', '.join(['"' + c.title + '"' for c in columns])
        for i in range(len(columns[0].data)):
            result += '[%s],\n' % (', '.join([str(c.data[i]) for c in columns]))

        result += """
        ]);

            var options = {
              title: '%s',
              //curveType: 'function',
              chartArea:{left:60,top:10,width:'65%%',height:'85%%'},
              colors: ['%s']
            };

            var chart = new google.visualization.LineChart(document.getElementById('%s'));
            chart.draw(data, options);
        }
        """ % (title, "','".join([self.color(c.chart.max_th if c.chart is not None else columns.index(c)-1) for c in columns[1:]]),div_id)

        return ChartView(self.database, self.host, title, div_id, result)

    def add_chart(self, report, columns, title):
        if len([c for c in columns if c.chart is not None])>0:
            report.add_chart_view(self.draw_chart([columns[0]]+[c for c in columns[1:] if c.chart.max_th in range(1,9+1)], title+" 01..9"))
            report.add_chart_view(self.draw_chart([columns[0]]+[c for c in columns[1:] if c.chart.max_th in range(7,20+1)], title+" 07..20"))
            report.add_chart_view(self.draw_chart([columns[0]]+[c for c in columns[1:] if c.chart.max_th in range(15,256+1)], title+" 15..256"))
        else:
            report.add_chart_view(self.draw_chart(columns, title))

    def render_group(self, report, time_line, group_list, meta_prefix, y_metric, threads_metric):
        self.add_chart(report, [time_line] + [y_metric(c) for c in group_list], meta_prefix + " /1 sec")
        #self.add_chart(report, [time_line.aggregate(10)] + [y_metric(c).aggregate(10) for c in group_list], meta_prefix+" /10 sec")
        self.add_chart(report, [
            meta_column([y_metric(c) for c in group_list], meta_prefix + ' Threads', threads_metric),
            meta_column([y_metric(c) for c in group_list], meta_prefix + ' ops avg', lambda c: c.avg),
            #meta_column([y_metric(c) for c in group_list], meta_prefix + ' ops sd', lambda c: c.sd),
            meta_column([y_metric(c) for c in group_list], meta_prefix + ' ops 90% range size', lambda c: c.upper_90-c.lower_90),
        ], meta_prefix+" Avg + 90% Range Size")
        if False:
            self.add_chart(report, [
                meta_column([y_metric(c) for c in group_list], meta_prefix + ' Threads', threads_metric),
                meta_column([y_metric(c) for c in group_list], meta_prefix + ' ops sd %',
                            lambda c: c.sd / max(c.avg, 1) * 100.0),
            ], meta_prefix + " SD/Avg %")
            self.add_chart(report, [
                meta_column([y_metric(c) for c in group_list], meta_prefix + ' Threads', threads_metric),
                meta_column([y_metric(c) for c in group_list], meta_prefix + ' ops avg', lambda c: c.avg),
                meta_column([y_metric(c) for c in group_list], meta_prefix + ' ops avg-sd*2',
                            lambda c: c.avg - c.sd * 2),
                meta_column([y_metric(c) for c in group_list], meta_prefix + ' ops avg+sd*2',
                            lambda c: c.avg + c.sd * 2),
            ], meta_prefix + " Avg + 95% SD Range")
        self.add_chart(report, [
            meta_column([y_metric(c) for c in group_list], meta_prefix + ' Threads', threads_metric),
            meta_column([y_metric(c) for c in group_list], meta_prefix + ' ops avg', lambda c: c.avg),
            meta_column([y_metric(c) for c in group_list], meta_prefix + ' ops upper', lambda c: c.upper_90),
            meta_column([y_metric(c) for c in group_list], meta_prefix + ' ops lower', lambda c: c.lower_90),
        ], meta_prefix+" Avg + 90% Range")

    def prepare_charts(self):
        chart_list = []
        for file_name in os.listdir('.'):
            if file_name.endswith('.csv'):
                chart_list.append(ChartData(file_name))

        chart_ins_list = [c for c in chart_list if c.w_ins > 0 and c.w_ins==c.write_th and c.read_th == 0]
        chart_up_tiny_list = [c for c in chart_list if c.w_up_tiny > 0 and c.w_up_tiny==c.write_th and c.read_th == 0]
        chart_up_wide_list = [c for c in chart_list if c.w_up_wide > 0 and c.w_up_wide==c.write_th and c.read_th == 0]
        chart_r_lite_list = [c for c in chart_list if c.r_lite > 0 and c.r_lite==c.read_th and c.write_th == 0]
        chart_r_heavy_list = [c for c in chart_list if c.r_heavy > 0 and c.r_heavy==c.read_th and c.write_th == 0]
        time_line = chart_list[0].time_line

        if len(chart_ins_list) > 0:
            self.render_group(self.report_write, time_line, chart_ins_list, 'Write Ins', lambda c: c.write_ops, lambda c: c.chart.write_th)
        if len(chart_up_tiny_list) > 0:
            self.render_group(self.report_write, time_line, chart_up_tiny_list, 'Write Up Tiny', lambda c: c.write_ops, lambda c: c.chart.write_th)
        if len(chart_up_wide_list) > 0:
            self.render_group(self.report_write, time_line, chart_up_wide_list, 'Write Up Wide', lambda c: c.write_ops, lambda c: c.chart.write_th)

        if len(chart_r_lite_list) > 0:
            self.render_group(self.report_read, time_line, chart_r_lite_list, 'Read by Id', lambda c: c.read_ops, lambda c: c.chart.read_th)
        if len(chart_r_heavy_list) > 0:
            self.render_group(self.report_read, time_line, chart_r_heavy_list, 'Read by Range', lambda c: c.read_ops, lambda c: c.chart.read_th)

    def generate_report_view(self):
        self.prepare_charts()
        return self.report_write


def meta_column(columns, title, metric):
    return ColumnData(None, title, [metric(c) for c in columns])


def save_charts(report_view, output_html):
    with open(output_html, 'w') as out:
        out.write("""<html>
        <head>
        <script type="text/javascript" src="https://www.google.com/jsapi"></script>
        <script type="text/javascript">
          google.load("visualization", "1", {packages:["corechart"]});
          google.setOnLoadCallback(function(){
        """)
        for chart_view in report_view.chart_views:
            out.write("         %s();\n" % chart_view.div_id)
        out.write("""
          });
          """)
        for chart_view in report_view.chart_views:
            out.write(chart_view.data)

        out.write("""
        </script>
        </head>
        <body>
            <table>
        """)

        for title in report_view.list_titles():
            out.write("<tr>\n")
            for profile in report_view.list_profiles():
                out.write("  <td style='vertical-align:text-top'>\n")
                out.write("    <p align='center'><b><i>%s</i><br/>%s</b></p>\n" % (profile, title, ))
                chart_view = report_view.get_chart_view(profile, title)
                if chart_view is not None:
                    out.write('     <div id="%s" style="width: 500; height: 300px;"></div>\n' % chart_view.div_id)
                out.write("  </td>\n")
            out.write("</tr>\n")

        out.write("""
            </table>
        </body>
    </html>""")


os.chdir('..')
report_write = ReportView()
report_read = ReportView()
home_dir = os.path.abspath('.')
data_dir = './data'
for database in os.listdir(data_dir):
    database_dir = os.path.join(data_dir, database)
    if not database.startswith('-') and os.path.isdir(database_dir):
        for profile in os.listdir(database_dir):
            profile_dir = os.path.join(database_dir, profile)
            if os.path.isdir(profile_dir):
                os.chdir(profile_dir)
                report_generator = ReportGenerator(database, profile)
                report_generator.generate_report_view()

                report_write.add_report_view(report_generator.report_write)
                report_read.add_report_view(report_generator.report_read)

                os.chdir(home_dir)

save_charts(report_write,'data/report-write.html')
save_charts(report_read,'data/report-read.html')