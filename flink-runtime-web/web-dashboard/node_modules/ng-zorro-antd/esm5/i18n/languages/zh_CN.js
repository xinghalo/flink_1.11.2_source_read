/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import Calendar from './calendar/zh_CN';
import DatePicker from './date-picker/zh_CN';
import Pagination from './pagination/zh_CN';
import TimePicker from './time-picker/zh_CN';
export default {
    locale: 'zh-cn',
    Pagination: Pagination,
    DatePicker: DatePicker,
    TimePicker: TimePicker,
    Calendar: Calendar,
    // locales for all comoponents
    global: {
        placeholder: '请选择',
    },
    Table: {
        filterTitle: '筛选',
        filterConfirm: '确定',
        filterReset: '重置',
        selectAll: '全选当页',
        selectInvert: '反选当页',
        sortTitle: '排序',
    },
    Modal: {
        okText: '确定',
        cancelText: '取消',
        justOkText: '知道了',
    },
    Popconfirm: {
        cancelText: '取消',
        okText: '确定',
    },
    Transfer: {
        searchPlaceholder: '请输入搜索内容',
        itemUnit: '项',
        itemsUnit: '项',
    },
    Upload: {
        uploading: '文件上传中',
        removeFile: '删除文件',
        uploadError: '上传错误',
        previewFile: '预览文件',
    },
    Empty: {
        description: '暂无数据',
    },
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiemhfQ04uanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiaTE4bi9sYW5ndWFnZXMvemhfQ04udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sUUFBUSxNQUFNLGtCQUFrQixDQUFDO0FBQ3hDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBQzdDLE9BQU8sVUFBVSxNQUFNLG9CQUFvQixDQUFDO0FBQzVDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBRTdDLGVBQWU7SUFDYixNQUFNLEVBQUUsT0FBTztJQUNmLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFFBQVEsVUFBQTs7SUFFUixNQUFNLEVBQUU7UUFDTixXQUFXLEVBQUUsS0FBSztLQUNuQjtJQUNELEtBQUssRUFBRTtRQUNMLFdBQVcsRUFBRSxJQUFJO1FBQ2pCLGFBQWEsRUFBRSxJQUFJO1FBQ25CLFdBQVcsRUFBRSxJQUFJO1FBQ2pCLFNBQVMsRUFBRSxNQUFNO1FBQ2pCLFlBQVksRUFBRSxNQUFNO1FBQ3BCLFNBQVMsRUFBRSxJQUFJO0tBQ2hCO0lBQ0QsS0FBSyxFQUFFO1FBQ0wsTUFBTSxFQUFFLElBQUk7UUFDWixVQUFVLEVBQUUsSUFBSTtRQUNoQixVQUFVLEVBQUUsS0FBSztLQUNsQjtJQUNELFVBQVUsRUFBRTtRQUNWLFVBQVUsRUFBRSxJQUFJO1FBQ2hCLE1BQU0sRUFBRSxJQUFJO0tBQ2I7SUFDRCxRQUFRLEVBQUU7UUFDUixpQkFBaUIsRUFBRSxTQUFTO1FBQzVCLFFBQVEsRUFBRSxHQUFHO1FBQ2IsU0FBUyxFQUFFLEdBQUc7S0FDZjtJQUNELE1BQU0sRUFBRTtRQUNOLFNBQVMsRUFBRSxPQUFPO1FBQ2xCLFVBQVUsRUFBRSxNQUFNO1FBQ2xCLFdBQVcsRUFBRSxNQUFNO1FBQ25CLFdBQVcsRUFBRSxNQUFNO0tBQ3BCO0lBQ0QsS0FBSyxFQUFFO1FBQ0wsV0FBVyxFQUFFLE1BQU07S0FDcEI7Q0FDRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IENhbGVuZGFyIGZyb20gJy4vY2FsZW5kYXIvemhfQ04nO1xuaW1wb3J0IERhdGVQaWNrZXIgZnJvbSAnLi9kYXRlLXBpY2tlci96aF9DTic7XG5pbXBvcnQgUGFnaW5hdGlvbiBmcm9tICcuL3BhZ2luYXRpb24vemhfQ04nO1xuaW1wb3J0IFRpbWVQaWNrZXIgZnJvbSAnLi90aW1lLXBpY2tlci96aF9DTic7XG5cbmV4cG9ydCBkZWZhdWx0IHtcbiAgbG9jYWxlOiAnemgtY24nLFxuICBQYWdpbmF0aW9uLFxuICBEYXRlUGlja2VyLFxuICBUaW1lUGlja2VyLFxuICBDYWxlbmRhcixcbiAgLy8gbG9jYWxlcyBmb3IgYWxsIGNvbW9wb25lbnRzXG4gIGdsb2JhbDoge1xuICAgIHBsYWNlaG9sZGVyOiAn6K+36YCJ5oupJyxcbiAgfSxcbiAgVGFibGU6IHtcbiAgICBmaWx0ZXJUaXRsZTogJ+etm+mAiScsXG4gICAgZmlsdGVyQ29uZmlybTogJ+ehruWumicsXG4gICAgZmlsdGVyUmVzZXQ6ICfph43nva4nLFxuICAgIHNlbGVjdEFsbDogJ+WFqOmAieW9k+mhtScsXG4gICAgc2VsZWN0SW52ZXJ0OiAn5Y+N6YCJ5b2T6aG1JyxcbiAgICBzb3J0VGl0bGU6ICfmjpLluo8nLFxuICB9LFxuICBNb2RhbDoge1xuICAgIG9rVGV4dDogJ+ehruWumicsXG4gICAgY2FuY2VsVGV4dDogJ+WPlua2iCcsXG4gICAganVzdE9rVGV4dDogJ+efpemBk+S6hicsXG4gIH0sXG4gIFBvcGNvbmZpcm06IHtcbiAgICBjYW5jZWxUZXh0OiAn5Y+W5raIJyxcbiAgICBva1RleHQ6ICfnoa7lrponLFxuICB9LFxuICBUcmFuc2Zlcjoge1xuICAgIHNlYXJjaFBsYWNlaG9sZGVyOiAn6K+36L6T5YWl5pCc57Si5YaF5a65JyxcbiAgICBpdGVtVW5pdDogJ+mhuScsXG4gICAgaXRlbXNVbml0OiAn6aG5JyxcbiAgfSxcbiAgVXBsb2FkOiB7XG4gICAgdXBsb2FkaW5nOiAn5paH5Lu25LiK5Lyg5LitJyxcbiAgICByZW1vdmVGaWxlOiAn5Yig6Zmk5paH5Lu2JyxcbiAgICB1cGxvYWRFcnJvcjogJ+S4iuS8oOmUmeivrycsXG4gICAgcHJldmlld0ZpbGU6ICfpooTop4jmlofku7YnLFxuICB9LFxuICBFbXB0eToge1xuICAgIGRlc2NyaXB0aW9uOiAn5pqC5peg5pWw5o2uJyxcbiAgfSxcbn07XG4iXX0=