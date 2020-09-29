/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import Calendar from './calendar/cs_CZ';
import DatePicker from './date-picker/cs_CZ';
import Pagination from './pagination/cs_CZ';
import TimePicker from './time-picker/cs_CZ';
export default {
    locale: 'cs',
    Pagination,
    DatePicker,
    TimePicker,
    Calendar,
    Table: {
        filterTitle: 'Filtr',
        filterConfirm: 'Potvrdit',
        filterReset: 'Obnovit',
    },
    Modal: {
        okText: 'Ok',
        cancelText: 'Storno',
        justOkText: 'Ok',
    },
    Popconfirm: {
        okText: 'Ok',
        cancelText: 'Storno',
    },
    Transfer: {
        searchPlaceholder: 'Vyhledávání',
        itemUnit: 'položka',
        itemsUnit: 'položek',
    },
    Upload: {
        uploading: 'Nahrávání...',
        removeFile: 'Odstranit soubor',
        uploadError: 'Chyba při nahrávání',
        previewFile: 'Zobrazit soubor',
    },
    Empty: {
        description: 'Žádná data',
    },
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY3NfQ1ouanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiaTE4bi9sYW5ndWFnZXMvY3NfQ1oudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sUUFBUSxNQUFNLGtCQUFrQixDQUFDO0FBQ3hDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBQzdDLE9BQU8sVUFBVSxNQUFNLG9CQUFvQixDQUFDO0FBQzVDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBRTdDLGVBQWU7SUFDYixNQUFNLEVBQUUsSUFBSTtJQUNaLFVBQVU7SUFDVixVQUFVO0lBQ1YsVUFBVTtJQUNWLFFBQVE7SUFDUixLQUFLLEVBQUU7UUFDTCxXQUFXLEVBQUUsT0FBTztRQUNwQixhQUFhLEVBQUUsVUFBVTtRQUN6QixXQUFXLEVBQUUsU0FBUztLQUN2QjtJQUNELEtBQUssRUFBRTtRQUNMLE1BQU0sRUFBRSxJQUFJO1FBQ1osVUFBVSxFQUFFLFFBQVE7UUFDcEIsVUFBVSxFQUFFLElBQUk7S0FDakI7SUFDRCxVQUFVLEVBQUU7UUFDVixNQUFNLEVBQUUsSUFBSTtRQUNaLFVBQVUsRUFBRSxRQUFRO0tBQ3JCO0lBQ0QsUUFBUSxFQUFFO1FBQ1IsaUJBQWlCLEVBQUUsYUFBYTtRQUNoQyxRQUFRLEVBQUUsU0FBUztRQUNuQixTQUFTLEVBQUUsU0FBUztLQUNyQjtJQUNELE1BQU0sRUFBRTtRQUNOLFNBQVMsRUFBRSxjQUFjO1FBQ3pCLFVBQVUsRUFBRSxrQkFBa0I7UUFDOUIsV0FBVyxFQUFFLHFCQUFxQjtRQUNsQyxXQUFXLEVBQUUsaUJBQWlCO0tBQy9CO0lBQ0QsS0FBSyxFQUFFO1FBQ0wsV0FBVyxFQUFFLFlBQVk7S0FDMUI7Q0FDRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IENhbGVuZGFyIGZyb20gJy4vY2FsZW5kYXIvY3NfQ1onO1xuaW1wb3J0IERhdGVQaWNrZXIgZnJvbSAnLi9kYXRlLXBpY2tlci9jc19DWic7XG5pbXBvcnQgUGFnaW5hdGlvbiBmcm9tICcuL3BhZ2luYXRpb24vY3NfQ1onO1xuaW1wb3J0IFRpbWVQaWNrZXIgZnJvbSAnLi90aW1lLXBpY2tlci9jc19DWic7XG5cbmV4cG9ydCBkZWZhdWx0IHtcbiAgbG9jYWxlOiAnY3MnLFxuICBQYWdpbmF0aW9uLFxuICBEYXRlUGlja2VyLFxuICBUaW1lUGlja2VyLFxuICBDYWxlbmRhcixcbiAgVGFibGU6IHtcbiAgICBmaWx0ZXJUaXRsZTogJ0ZpbHRyJyxcbiAgICBmaWx0ZXJDb25maXJtOiAnUG90dnJkaXQnLFxuICAgIGZpbHRlclJlc2V0OiAnT2Jub3ZpdCcsXG4gIH0sXG4gIE1vZGFsOiB7XG4gICAgb2tUZXh0OiAnT2snLFxuICAgIGNhbmNlbFRleHQ6ICdTdG9ybm8nLFxuICAgIGp1c3RPa1RleHQ6ICdPaycsXG4gIH0sXG4gIFBvcGNvbmZpcm06IHtcbiAgICBva1RleHQ6ICdPaycsXG4gICAgY2FuY2VsVGV4dDogJ1N0b3JubycsXG4gIH0sXG4gIFRyYW5zZmVyOiB7XG4gICAgc2VhcmNoUGxhY2Vob2xkZXI6ICdWeWhsZWTDoXbDoW7DrScsXG4gICAgaXRlbVVuaXQ6ICdwb2xvxb5rYScsXG4gICAgaXRlbXNVbml0OiAncG9sb8W+ZWsnLFxuICB9LFxuICBVcGxvYWQ6IHtcbiAgICB1cGxvYWRpbmc6ICdOYWhyw6F2w6Fuw60uLi4nLFxuICAgIHJlbW92ZUZpbGU6ICdPZHN0cmFuaXQgc291Ym9yJyxcbiAgICB1cGxvYWRFcnJvcjogJ0NoeWJhIHDFmWkgbmFocsOhdsOhbsOtJyxcbiAgICBwcmV2aWV3RmlsZTogJ1pvYnJheml0IHNvdWJvcicsXG4gIH0sXG4gIEVtcHR5OiB7XG4gICAgZGVzY3JpcHRpb246ICfFvcOhZG7DoSBkYXRhJyxcbiAgfSxcbn07XG4iXX0=