/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import Calendar from './calendar/en_GB';
import DatePicker from './date-picker/en_GB';
import Pagination from './pagination/en_GB';
import TimePicker from './time-picker/en_GB';
export default {
    locale: 'en-gb',
    Pagination: Pagination,
    DatePicker: DatePicker,
    TimePicker: TimePicker,
    Calendar: Calendar,
    Table: {
        filterTitle: 'Filter menu',
        filterConfirm: 'OK',
        filterReset: 'Reset',
        selectAll: 'Select current page',
        selectInvert: 'Invert current page',
    },
    Modal: {
        okText: 'OK',
        cancelText: 'Cancel',
        justOkText: 'OK',
    },
    Popconfirm: {
        okText: 'OK',
        cancelText: 'Cancel',
    },
    Transfer: {
        searchPlaceholder: 'Search here',
        itemUnit: 'item',
        itemsUnit: 'items',
    },
    Upload: {
        uploading: 'Uploading...',
        removeFile: 'Remove file',
        uploadError: 'Upload error',
        previewFile: 'Preview file',
    },
    Empty: {
        description: 'No data',
    },
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZW5fR0IuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiaTE4bi9sYW5ndWFnZXMvZW5fR0IudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sUUFBUSxNQUFNLGtCQUFrQixDQUFDO0FBQ3hDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBQzdDLE9BQU8sVUFBVSxNQUFNLG9CQUFvQixDQUFDO0FBQzVDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBRTdDLGVBQWU7SUFDYixNQUFNLEVBQUUsT0FBTztJQUNmLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFFBQVEsVUFBQTtJQUNSLEtBQUssRUFBRTtRQUNMLFdBQVcsRUFBRSxhQUFhO1FBQzFCLGFBQWEsRUFBRSxJQUFJO1FBQ25CLFdBQVcsRUFBRSxPQUFPO1FBQ3BCLFNBQVMsRUFBRSxxQkFBcUI7UUFDaEMsWUFBWSxFQUFFLHFCQUFxQjtLQUNwQztJQUNELEtBQUssRUFBRTtRQUNMLE1BQU0sRUFBRSxJQUFJO1FBQ1osVUFBVSxFQUFFLFFBQVE7UUFDcEIsVUFBVSxFQUFFLElBQUk7S0FDakI7SUFDRCxVQUFVLEVBQUU7UUFDVixNQUFNLEVBQUUsSUFBSTtRQUNaLFVBQVUsRUFBRSxRQUFRO0tBQ3JCO0lBQ0QsUUFBUSxFQUFFO1FBQ1IsaUJBQWlCLEVBQUUsYUFBYTtRQUNoQyxRQUFRLEVBQUUsTUFBTTtRQUNoQixTQUFTLEVBQUUsT0FBTztLQUNuQjtJQUNELE1BQU0sRUFBRTtRQUNOLFNBQVMsRUFBRSxjQUFjO1FBQ3pCLFVBQVUsRUFBRSxhQUFhO1FBQ3pCLFdBQVcsRUFBRSxjQUFjO1FBQzNCLFdBQVcsRUFBRSxjQUFjO0tBQzVCO0lBQ0QsS0FBSyxFQUFFO1FBQ0wsV0FBVyxFQUFFLFNBQVM7S0FDdkI7Q0FDRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IENhbGVuZGFyIGZyb20gJy4vY2FsZW5kYXIvZW5fR0InO1xuaW1wb3J0IERhdGVQaWNrZXIgZnJvbSAnLi9kYXRlLXBpY2tlci9lbl9HQic7XG5pbXBvcnQgUGFnaW5hdGlvbiBmcm9tICcuL3BhZ2luYXRpb24vZW5fR0InO1xuaW1wb3J0IFRpbWVQaWNrZXIgZnJvbSAnLi90aW1lLXBpY2tlci9lbl9HQic7XG5cbmV4cG9ydCBkZWZhdWx0IHtcbiAgbG9jYWxlOiAnZW4tZ2InLFxuICBQYWdpbmF0aW9uLFxuICBEYXRlUGlja2VyLFxuICBUaW1lUGlja2VyLFxuICBDYWxlbmRhcixcbiAgVGFibGU6IHtcbiAgICBmaWx0ZXJUaXRsZTogJ0ZpbHRlciBtZW51JyxcbiAgICBmaWx0ZXJDb25maXJtOiAnT0snLFxuICAgIGZpbHRlclJlc2V0OiAnUmVzZXQnLFxuICAgIHNlbGVjdEFsbDogJ1NlbGVjdCBjdXJyZW50IHBhZ2UnLFxuICAgIHNlbGVjdEludmVydDogJ0ludmVydCBjdXJyZW50IHBhZ2UnLFxuICB9LFxuICBNb2RhbDoge1xuICAgIG9rVGV4dDogJ09LJyxcbiAgICBjYW5jZWxUZXh0OiAnQ2FuY2VsJyxcbiAgICBqdXN0T2tUZXh0OiAnT0snLFxuICB9LFxuICBQb3Bjb25maXJtOiB7XG4gICAgb2tUZXh0OiAnT0snLFxuICAgIGNhbmNlbFRleHQ6ICdDYW5jZWwnLFxuICB9LFxuICBUcmFuc2Zlcjoge1xuICAgIHNlYXJjaFBsYWNlaG9sZGVyOiAnU2VhcmNoIGhlcmUnLFxuICAgIGl0ZW1Vbml0OiAnaXRlbScsXG4gICAgaXRlbXNVbml0OiAnaXRlbXMnLFxuICB9LFxuICBVcGxvYWQ6IHtcbiAgICB1cGxvYWRpbmc6ICdVcGxvYWRpbmcuLi4nLFxuICAgIHJlbW92ZUZpbGU6ICdSZW1vdmUgZmlsZScsXG4gICAgdXBsb2FkRXJyb3I6ICdVcGxvYWQgZXJyb3InLFxuICAgIHByZXZpZXdGaWxlOiAnUHJldmlldyBmaWxlJyxcbiAgfSxcbiAgRW1wdHk6IHtcbiAgICBkZXNjcmlwdGlvbjogJ05vIGRhdGEnLFxuICB9LFxufTtcbiJdfQ==