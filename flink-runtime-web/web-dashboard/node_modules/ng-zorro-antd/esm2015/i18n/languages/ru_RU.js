/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import Calendar from './calendar/ru_RU';
import DatePicker from './date-picker/ru_RU';
import Pagination from './pagination/ru_RU';
import TimePicker from './time-picker/ru_RU';
export default {
    locale: 'ru',
    Pagination,
    DatePicker,
    TimePicker,
    Calendar,
    Table: {
        filterTitle: 'Фильтр',
        filterConfirm: 'OK',
        filterReset: 'Сбросить',
        selectAll: 'Выбрать всё',
        selectInvert: 'Инвертировать выбор',
    },
    Modal: {
        okText: 'OK',
        cancelText: 'Отмена',
        justOkText: 'OK',
    },
    Popconfirm: {
        okText: 'OK',
        cancelText: 'Отмена',
    },
    Transfer: {
        searchPlaceholder: 'Поиск',
        itemUnit: 'элем.',
        itemsUnit: 'элем.',
    },
    Upload: {
        uploading: 'Загрузка...',
        removeFile: 'Удалить файл',
        uploadError: 'При загрузке произошла ошибка',
        previewFile: 'Предпросмотр файла',
    },
    Empty: {
        description: 'Нет данных',
    },
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicnVfUlUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiaTE4bi9sYW5ndWFnZXMvcnVfUlUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sUUFBUSxNQUFNLGtCQUFrQixDQUFDO0FBQ3hDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBQzdDLE9BQU8sVUFBVSxNQUFNLG9CQUFvQixDQUFDO0FBQzVDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBRTdDLGVBQWU7SUFDYixNQUFNLEVBQUUsSUFBSTtJQUNaLFVBQVU7SUFDVixVQUFVO0lBQ1YsVUFBVTtJQUNWLFFBQVE7SUFDUixLQUFLLEVBQUU7UUFDTCxXQUFXLEVBQUUsUUFBUTtRQUNyQixhQUFhLEVBQUUsSUFBSTtRQUNuQixXQUFXLEVBQUUsVUFBVTtRQUN2QixTQUFTLEVBQUUsYUFBYTtRQUN4QixZQUFZLEVBQUUscUJBQXFCO0tBQ3BDO0lBQ0QsS0FBSyxFQUFFO1FBQ0wsTUFBTSxFQUFFLElBQUk7UUFDWixVQUFVLEVBQUUsUUFBUTtRQUNwQixVQUFVLEVBQUUsSUFBSTtLQUNqQjtJQUNELFVBQVUsRUFBRTtRQUNWLE1BQU0sRUFBRSxJQUFJO1FBQ1osVUFBVSxFQUFFLFFBQVE7S0FDckI7SUFDRCxRQUFRLEVBQUU7UUFDUixpQkFBaUIsRUFBRSxPQUFPO1FBQzFCLFFBQVEsRUFBRSxPQUFPO1FBQ2pCLFNBQVMsRUFBRSxPQUFPO0tBQ25CO0lBQ0QsTUFBTSxFQUFFO1FBQ04sU0FBUyxFQUFFLGFBQWE7UUFDeEIsVUFBVSxFQUFFLGNBQWM7UUFDMUIsV0FBVyxFQUFFLCtCQUErQjtRQUM1QyxXQUFXLEVBQUUsb0JBQW9CO0tBQ2xDO0lBQ0QsS0FBSyxFQUFFO1FBQ0wsV0FBVyxFQUFFLFlBQVk7S0FDMUI7Q0FDRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IENhbGVuZGFyIGZyb20gJy4vY2FsZW5kYXIvcnVfUlUnO1xuaW1wb3J0IERhdGVQaWNrZXIgZnJvbSAnLi9kYXRlLXBpY2tlci9ydV9SVSc7XG5pbXBvcnQgUGFnaW5hdGlvbiBmcm9tICcuL3BhZ2luYXRpb24vcnVfUlUnO1xuaW1wb3J0IFRpbWVQaWNrZXIgZnJvbSAnLi90aW1lLXBpY2tlci9ydV9SVSc7XG5cbmV4cG9ydCBkZWZhdWx0IHtcbiAgbG9jYWxlOiAncnUnLFxuICBQYWdpbmF0aW9uLFxuICBEYXRlUGlja2VyLFxuICBUaW1lUGlja2VyLFxuICBDYWxlbmRhcixcbiAgVGFibGU6IHtcbiAgICBmaWx0ZXJUaXRsZTogJ9Ck0LjQu9GM0YLRgCcsXG4gICAgZmlsdGVyQ29uZmlybTogJ09LJyxcbiAgICBmaWx0ZXJSZXNldDogJ9Ch0LHRgNC+0YHQuNGC0YwnLFxuICAgIHNlbGVjdEFsbDogJ9CS0YvQsdGA0LDRgtGMINCy0YHRkScsXG4gICAgc2VsZWN0SW52ZXJ0OiAn0JjQvdCy0LXRgNGC0LjRgNC+0LLQsNGC0Ywg0LLRi9Cx0L7RgCcsXG4gIH0sXG4gIE1vZGFsOiB7XG4gICAgb2tUZXh0OiAnT0snLFxuICAgIGNhbmNlbFRleHQ6ICfQntGC0LzQtdC90LAnLFxuICAgIGp1c3RPa1RleHQ6ICdPSycsXG4gIH0sXG4gIFBvcGNvbmZpcm06IHtcbiAgICBva1RleHQ6ICdPSycsXG4gICAgY2FuY2VsVGV4dDogJ9Ce0YLQvNC10L3QsCcsXG4gIH0sXG4gIFRyYW5zZmVyOiB7XG4gICAgc2VhcmNoUGxhY2Vob2xkZXI6ICfQn9C+0LjRgdC6JyxcbiAgICBpdGVtVW5pdDogJ9GN0LvQtdC8LicsXG4gICAgaXRlbXNVbml0OiAn0Y3Qu9C10LwuJyxcbiAgfSxcbiAgVXBsb2FkOiB7XG4gICAgdXBsb2FkaW5nOiAn0JfQsNCz0YDRg9C30LrQsC4uLicsXG4gICAgcmVtb3ZlRmlsZTogJ9Cj0LTQsNC70LjRgtGMINGE0LDQudC7JyxcbiAgICB1cGxvYWRFcnJvcjogJ9Cf0YDQuCDQt9Cw0LPRgNGD0LfQutC1INC/0YDQvtC40LfQvtGI0LvQsCDQvtGI0LjQsdC60LAnLFxuICAgIHByZXZpZXdGaWxlOiAn0J/RgNC10LTQv9GA0L7RgdC80L7RgtGAINGE0LDQudC70LAnLFxuICB9LFxuICBFbXB0eToge1xuICAgIGRlc2NyaXB0aW9uOiAn0J3QtdGCINC00LDQvdC90YvRhScsXG4gIH0sXG59O1xuIl19