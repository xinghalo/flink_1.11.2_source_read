/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import Calendar from './calendar/sk_SK';
import DatePicker from './date-picker/sk_SK';
import Pagination from './pagination/sk_SK';
import TimePicker from './time-picker/sk_SK';
export default {
    locale: 'sk',
    Pagination,
    DatePicker,
    TimePicker,
    Calendar,
    Table: {
        filterTitle: 'Filter',
        filterConfirm: 'OK',
        filterReset: 'Obnoviť',
        selectAll: 'Vybrať všetko',
        selectInvert: 'Vybrať opačné',
    },
    Modal: {
        okText: 'OK',
        cancelText: 'Zrušiť',
        justOkText: 'OK',
    },
    Popconfirm: {
        okText: 'OK',
        cancelText: 'Zrušiť',
    },
    Transfer: {
        searchPlaceholder: 'Vyhľadávanie',
        itemUnit: 'položka',
        itemsUnit: 'položiek',
    },
    Upload: {
        uploading: 'Nahrávanie...',
        removeFile: 'Odstrániť súbor',
        uploadError: 'Chyba pri nahrávaní',
        previewFile: 'Zobraziť súbor',
    },
    Empty: {
        description: 'Žiadne dáta',
    },
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2tfU0suanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiaTE4bi9sYW5ndWFnZXMvc2tfU0sudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sUUFBUSxNQUFNLGtCQUFrQixDQUFDO0FBQ3hDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBQzdDLE9BQU8sVUFBVSxNQUFNLG9CQUFvQixDQUFDO0FBQzVDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBRTdDLGVBQWU7SUFDYixNQUFNLEVBQUUsSUFBSTtJQUNaLFVBQVU7SUFDVixVQUFVO0lBQ1YsVUFBVTtJQUNWLFFBQVE7SUFDUixLQUFLLEVBQUU7UUFDTCxXQUFXLEVBQUUsUUFBUTtRQUNyQixhQUFhLEVBQUUsSUFBSTtRQUNuQixXQUFXLEVBQUUsU0FBUztRQUN0QixTQUFTLEVBQUUsZUFBZTtRQUMxQixZQUFZLEVBQUUsZUFBZTtLQUM5QjtJQUNELEtBQUssRUFBRTtRQUNMLE1BQU0sRUFBRSxJQUFJO1FBQ1osVUFBVSxFQUFFLFFBQVE7UUFDcEIsVUFBVSxFQUFFLElBQUk7S0FDakI7SUFDRCxVQUFVLEVBQUU7UUFDVixNQUFNLEVBQUUsSUFBSTtRQUNaLFVBQVUsRUFBRSxRQUFRO0tBQ3JCO0lBQ0QsUUFBUSxFQUFFO1FBQ1IsaUJBQWlCLEVBQUUsY0FBYztRQUNqQyxRQUFRLEVBQUUsU0FBUztRQUNuQixTQUFTLEVBQUUsVUFBVTtLQUN0QjtJQUNELE1BQU0sRUFBRTtRQUNOLFNBQVMsRUFBRSxlQUFlO1FBQzFCLFVBQVUsRUFBRSxpQkFBaUI7UUFDN0IsV0FBVyxFQUFFLHFCQUFxQjtRQUNsQyxXQUFXLEVBQUUsZ0JBQWdCO0tBQzlCO0lBQ0QsS0FBSyxFQUFFO1FBQ0wsV0FBVyxFQUFFLGFBQWE7S0FDM0I7Q0FDRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IENhbGVuZGFyIGZyb20gJy4vY2FsZW5kYXIvc2tfU0snO1xuaW1wb3J0IERhdGVQaWNrZXIgZnJvbSAnLi9kYXRlLXBpY2tlci9za19TSyc7XG5pbXBvcnQgUGFnaW5hdGlvbiBmcm9tICcuL3BhZ2luYXRpb24vc2tfU0snO1xuaW1wb3J0IFRpbWVQaWNrZXIgZnJvbSAnLi90aW1lLXBpY2tlci9za19TSyc7XG5cbmV4cG9ydCBkZWZhdWx0IHtcbiAgbG9jYWxlOiAnc2snLFxuICBQYWdpbmF0aW9uLFxuICBEYXRlUGlja2VyLFxuICBUaW1lUGlja2VyLFxuICBDYWxlbmRhcixcbiAgVGFibGU6IHtcbiAgICBmaWx0ZXJUaXRsZTogJ0ZpbHRlcicsXG4gICAgZmlsdGVyQ29uZmlybTogJ09LJyxcbiAgICBmaWx0ZXJSZXNldDogJ09ibm92acWlJyxcbiAgICBzZWxlY3RBbGw6ICdWeWJyYcWlIHbFoWV0a28nLFxuICAgIHNlbGVjdEludmVydDogJ1Z5YnJhxaUgb3BhxI1uw6knLFxuICB9LFxuICBNb2RhbDoge1xuICAgIG9rVGV4dDogJ09LJyxcbiAgICBjYW5jZWxUZXh0OiAnWnJ1xaFpxaUnLFxuICAgIGp1c3RPa1RleHQ6ICdPSycsXG4gIH0sXG4gIFBvcGNvbmZpcm06IHtcbiAgICBva1RleHQ6ICdPSycsXG4gICAgY2FuY2VsVGV4dDogJ1pydcWhacWlJyxcbiAgfSxcbiAgVHJhbnNmZXI6IHtcbiAgICBzZWFyY2hQbGFjZWhvbGRlcjogJ1Z5aMS+YWTDoXZhbmllJyxcbiAgICBpdGVtVW5pdDogJ3BvbG/FvmthJyxcbiAgICBpdGVtc1VuaXQ6ICdwb2xvxb5pZWsnLFxuICB9LFxuICBVcGxvYWQ6IHtcbiAgICB1cGxvYWRpbmc6ICdOYWhyw6F2YW5pZS4uLicsXG4gICAgcmVtb3ZlRmlsZTogJ09kc3Ryw6FuacWlIHPDumJvcicsXG4gICAgdXBsb2FkRXJyb3I6ICdDaHliYSBwcmkgbmFocsOhdmFuw60nLFxuICAgIHByZXZpZXdGaWxlOiAnWm9icmF6acWlIHPDumJvcicsXG4gIH0sXG4gIEVtcHR5OiB7XG4gICAgZGVzY3JpcHRpb246ICfFvWlhZG5lIGTDoXRhJyxcbiAgfSxcbn07XG4iXX0=