/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import Calendar from './calendar/fr_BE';
import DatePicker from './date-picker/fr_BE';
import Pagination from './pagination/fr_BE';
import TimePicker from './time-picker/fr_BE';
export default {
    locale: 'fr',
    Pagination: Pagination,
    DatePicker: DatePicker,
    TimePicker: TimePicker,
    Calendar: Calendar,
    Table: {
        filterTitle: 'Filtrer',
        filterConfirm: 'OK',
        filterReset: 'Réinitialiser',
    },
    Modal: {
        okText: 'OK',
        cancelText: 'Annuler',
        justOkText: 'OK',
    },
    Popconfirm: {
        okText: 'OK',
        cancelText: 'Annuler',
    },
    Transfer: {
        searchPlaceholder: 'Recherche',
        itemUnit: 'élément',
        itemsUnit: 'éléments',
    },
    Empty: {
        description: 'Aucune donnée',
    },
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZnJfQkUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiaTE4bi9sYW5ndWFnZXMvZnJfQkUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sUUFBUSxNQUFNLGtCQUFrQixDQUFDO0FBQ3hDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBQzdDLE9BQU8sVUFBVSxNQUFNLG9CQUFvQixDQUFDO0FBQzVDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBRTdDLGVBQWU7SUFDYixNQUFNLEVBQUUsSUFBSTtJQUNaLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFFBQVEsVUFBQTtJQUNSLEtBQUssRUFBRTtRQUNMLFdBQVcsRUFBRSxTQUFTO1FBQ3RCLGFBQWEsRUFBRSxJQUFJO1FBQ25CLFdBQVcsRUFBRSxlQUFlO0tBQzdCO0lBQ0QsS0FBSyxFQUFFO1FBQ0wsTUFBTSxFQUFFLElBQUk7UUFDWixVQUFVLEVBQUUsU0FBUztRQUNyQixVQUFVLEVBQUUsSUFBSTtLQUNqQjtJQUNELFVBQVUsRUFBRTtRQUNWLE1BQU0sRUFBRSxJQUFJO1FBQ1osVUFBVSxFQUFFLFNBQVM7S0FDdEI7SUFDRCxRQUFRLEVBQUU7UUFDUixpQkFBaUIsRUFBRSxXQUFXO1FBQzlCLFFBQVEsRUFBRSxTQUFTO1FBQ25CLFNBQVMsRUFBRSxVQUFVO0tBQ3RCO0lBQ0QsS0FBSyxFQUFFO1FBQ0wsV0FBVyxFQUFFLGVBQWU7S0FDN0I7Q0FDRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IENhbGVuZGFyIGZyb20gJy4vY2FsZW5kYXIvZnJfQkUnO1xuaW1wb3J0IERhdGVQaWNrZXIgZnJvbSAnLi9kYXRlLXBpY2tlci9mcl9CRSc7XG5pbXBvcnQgUGFnaW5hdGlvbiBmcm9tICcuL3BhZ2luYXRpb24vZnJfQkUnO1xuaW1wb3J0IFRpbWVQaWNrZXIgZnJvbSAnLi90aW1lLXBpY2tlci9mcl9CRSc7XG5cbmV4cG9ydCBkZWZhdWx0IHtcbiAgbG9jYWxlOiAnZnInLFxuICBQYWdpbmF0aW9uLFxuICBEYXRlUGlja2VyLFxuICBUaW1lUGlja2VyLFxuICBDYWxlbmRhcixcbiAgVGFibGU6IHtcbiAgICBmaWx0ZXJUaXRsZTogJ0ZpbHRyZXInLFxuICAgIGZpbHRlckNvbmZpcm06ICdPSycsXG4gICAgZmlsdGVyUmVzZXQ6ICdSw6lpbml0aWFsaXNlcicsXG4gIH0sXG4gIE1vZGFsOiB7XG4gICAgb2tUZXh0OiAnT0snLFxuICAgIGNhbmNlbFRleHQ6ICdBbm51bGVyJyxcbiAgICBqdXN0T2tUZXh0OiAnT0snLFxuICB9LFxuICBQb3Bjb25maXJtOiB7XG4gICAgb2tUZXh0OiAnT0snLFxuICAgIGNhbmNlbFRleHQ6ICdBbm51bGVyJyxcbiAgfSxcbiAgVHJhbnNmZXI6IHtcbiAgICBzZWFyY2hQbGFjZWhvbGRlcjogJ1JlY2hlcmNoZScsXG4gICAgaXRlbVVuaXQ6ICfDqWzDqW1lbnQnLFxuICAgIGl0ZW1zVW5pdDogJ8OpbMOpbWVudHMnLFxuICB9LFxuICBFbXB0eToge1xuICAgIGRlc2NyaXB0aW9uOiAnQXVjdW5lIGRvbm7DqWUnLFxuICB9LFxufTtcbiJdfQ==